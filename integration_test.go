// +build integration

package amqp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"pack.ag/amqp"

	"github.com/Azure/azure-sdk-for-go/arm/servicebus"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
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
			data:  []string{"Hello there!"},
		},
		{
			label: "3 roundtrip, small payload",
			data: []string{
				"Hey there!",
				"Hi there!",
				"Ho there!",
			},
		},
		{
			label: "1000 roundtrip, small payload",
			data: repeatStrings(100,
				"Hey there!",
				"Hi there!",
				"Ho there!",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			// Create client
			client, err := amqp.Dial("amqps://"+namespace+".servicebus.windows.net",
				amqp.ConnSASLPlain(accessKeyName, accessKey),
			)
			if err != nil {
				t.Fatal(err)
			}
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

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			for _, data := range tt.data {
				err = sender.Send(ctx, &amqp.Message{
					Data: []byte(data),
				})
				if err != nil {
					t.Fatal(fmt.Sprintf("%+v", err))
				}
			}

			// Create a receiver
			receiver, err := session.NewReceiver(
				amqp.LinkSource(queueName),
				amqp.LinkCredit(10),
				amqp.LinkBatching(false),
			)
			if err != nil {
				t.Fatal(err)
			}

			for _, data := range tt.data {
				msg, err := receiver.Receive(ctx)
				if err != nil {
					t.Fatal(err)
				}

				// Accept message
				msg.Accept()

				if !bytes.Equal([]byte(data), msg.Data) {
					t.Errorf("Expected received message to be %v, but it was %v", string(data), string(msg.Data))
				}
			}

			q, err := queuesClient.Get(resourceGroup, namespace, queueName)
			if err != nil {
				t.Fatal(err)
			}

			if amc := *q.CountDetails.ActiveMessageCount; amc != 0 {
				t.Fatalf("Expected ActiveMessageCount to be 0, but it was %d", amc)
			}
		})
	}
}

func dump(i interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	enc.Encode(i)
}

func newTestQueue(tb testing.TB, suffix string) (string, servicebus.QueuesClient, func()) {
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
