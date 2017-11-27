// +build integration

package amqp_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/servicebus"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/fortytw2/leaktest"
	"pack.ag/amqp"
)

var (
	subscriptionID = mustGetenv("AZURE_SUBSCRIPTION_ID")
	resourceGroup  = mustGetenv("AZURE_RESOURCE_GROUP")
	tenantID       = mustGetenv("AZURE_TENANT_ID")
	clientID       = mustGetenv("AZURE_CLIENT_ID")
	clientSecret   = mustGetenv("AZURE_CLIENT_SECRET")
	namespace      = mustGetenv("SERVICEBUS_NAMESPACE")
	accessKeyName  = mustGetenv("SERVICEBUS_ACCESS_KEY_NAME")
	accessKey      = mustGetenv("SERVICEBUS_ACCESS_KEY")

	tlsKeyLog = flag.String("tlskeylog", "", "path to write the TLS key log")
)

func TestIntegrationRoundTrip(t *testing.T) {
	queueName, queuesClient, cleanup := newTestQueue(t, "receive")
	defer cleanup()

	tests := []struct {
		label string
		data  []string
	}{
		{
			label: "1 roundtrip, small payload",
			data:  []string{"1Hello there!"},
		},
		{
			label: "3 roundtrip, small payload",
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label: "1000 roundtrip, small payload",
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.Check(t)

			// Create client
			client := newClient(t)
			defer client.Close()

			// Open a session
			session, err := client.NewSession()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			sender, err := session.NewSender(
				amqp.LinkSource(queueName),
			)
			if err != nil {
				t.Fatal(err)
			}

			// Perform test concurrently for speed and to catch races
			var wg sync.WaitGroup
			wg.Add(2)

			var sendErr error
			go func() {
				defer wg.Done()
				defer sender.Close()

				for i, data := range tt.data {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err = sender.Send(ctx, &amqp.Message{
						Data: []byte(data),
					})
					cancel()
					if err != nil {
						sendErr = fmt.Errorf("Error after %d sends: %v", i, err)
						return
					}
				}
			}()

			var receiveErr error
			go func() {
				defer wg.Done()

				// Create a receiver
				receiver, err := session.NewReceiver(
					amqp.LinkSource(queueName),
					amqp.LinkCredit(10),
					amqp.LinkBatching(false),
				)
				if err != nil {
					receiveErr = err
					return
				}
				defer receiver.Close()

				for i, data := range tt.data {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					msg, err := receiver.Receive(ctx)
					cancel()
					if err != nil {
						receiveErr = fmt.Errorf("Error after %d receives: %v", i, err)
						return
					}

					// Accept message
					msg.Accept()

					if !bytes.Equal([]byte(data), msg.Data) {
						receiveErr = fmt.Errorf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.Data))
					}
				}
			}()

			wg.Wait()

			if sendErr != nil || receiveErr != nil {
				t.Error("Send error:", sendErr)
				t.Fatal("Receive error:", receiveErr)
			}

			client.Close() // close before leak check

			checkLeaks() // this is done here because queuesClient starts additional goroutines

			q, err := queuesClient.Get(resourceGroup, namespace, queueName)
			if err != nil {
				t.Fatal(err)
			}

			if amc := *q.CountDetails.ActiveMessageCount; amc != 0 {
				t.Fatalf("Expected ActiveMessageCount to be 0, but it was %d", amc)
			}

			if dead := *q.CountDetails.DeadLetterMessageCount; dead > 0 {
				t.Fatal("Expected DeadLetterMessageCount to be 0, but it was %d", dead)
			}
		})
	}
}

func dump(i interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	enc.Encode(i)
}

func newClient(t testing.TB, opts ...amqp.ConnOption) *amqp.Client {
	opts = append(opts,
		amqp.ConnSASLPlain(accessKeyName, accessKey),
	)

	if *tlsKeyLog == "" {
		client, err := amqp.Dial("amqps://"+namespace+".servicebus.windows.net", opts...)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}

	f, err := os.OpenFile(*tlsKeyLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}

	tlsConfig := &tls.Config{
		KeyLogWriter: f,
	}

	tlsConn, err := tls.Dial("tcp", namespace+".servicebus.windows.net:5671", tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

	opts = append(opts,
		amqp.ConnServerHostname(namespace+".servicebus.windows.net"),
	)
	client, err := amqp.New(tlsConn, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newTestQueue(tb testing.TB, suffix string) (string, servicebus.QueuesClient, func()) {
	// TODO: Add entropy to queue name to prevent conflicts with multiple CI/local runs.
	queueName := "integration-" + suffix

	oauthConfig, err := adal.NewOAuthConfig(azure.PublicCloud.ActiveDirectoryEndpoint, tenantID)
	if err != nil {
		tb.Fatal(err)
	}
	token, err := adal.NewServicePrincipalToken(*oauthConfig, clientID, clientSecret, azure.PublicCloud.ResourceManagerEndpoint)
	if err != nil {
		tb.Fatal(err)
	}

	queuesClient := servicebus.NewQueuesClient(subscriptionID)
	queuesClient.Authorizer = autorest.NewBearerAuthorizer(token)

	params := servicebus.SBQueue{}
	_, err = queuesClient.CreateOrUpdate(resourceGroup, namespace, queueName, params)
	if err != nil {
		tb.Fatal(err)
	}

	cleanup := func() {
		_, err = queuesClient.Delete(resourceGroup, namespace, queueName)
		if err != nil {
			tb.Logf("Unable to remove queue: %s - %v", queueName, err)
		}
	}

	return queueName, queuesClient, cleanup
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("Environment variable '" + key + "' required for integration tests.")
	}
	return v
}

func repeatStrings(count int, strs ...string) []string {
	var out []string
	for i := 0; i < count; i += len(strs) {
		out = append(out, strs...)
	}
	return out[:count]
}
