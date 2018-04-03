// +build integration

package amqp_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	"github.com/Azure/azure-sdk-for-go/services/servicebus/mgmt/2017-04-01/servicebus"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/fortytw2/leaktest"
	"github.com/vcabbage/amqp"
	"github.com/vcabbage/amqp/internal/testconn"
)

func init() {
	// rand used to generate queue names, non-determinism is fine for this use
	rand.Seed(time.Now().UnixNano())
}

var (
	isForkPR        = os.Getenv("CI") != "" && os.Getenv("SERVICEBUS_ACCESS_KEY") == ""
	subscriptionID  = mustGetenv("AZURE_SUBSCRIPTION_ID")
	resourceGroup   = mustGetenv("AZURE_RESOURCE_GROUP")
	tenantID        = mustGetenv("AZURE_TENANT_ID")
	clientID        = mustGetenv("AZURE_CLIENT_ID")
	clientSecret    = mustGetenv("AZURE_CLIENT_SECRET")
	namespace       = mustGetenv("SERVICEBUS_NAMESPACE")
	accessKeyName   = mustGetenv("SERVICEBUS_ACCESS_KEY_NAME")
	accessKey       = mustGetenv("SERVICEBUS_ACCESS_KEY")
	ehNamespace     = mustGetenv("EVENTHUB_NAMESPACE")
	ehAccessKeyName = mustGetenv("EVENTHUB_ACCESS_KEY_NAME")
	ehAccessKey     = mustGetenv("EVENTHUB_ACCESS_KEY")

	tlsKeyLog = flag.String("tlskeylog", "", "path to write the TLS key log")
	recordDir = flag.String("recorddir", "", "directory to write connection records to")
)

func TestIntegrationRoundTrip(t *testing.T) {
	queueName, queuesClient, cleanup := newTestQueue(t, "receive")
	defer cleanup()

	tests := []struct {
		label    string
		sessions int
		data     []string
	}{
		{
			label:    "1 roundtrip, small payload",
			sessions: 1,
			data:     []string{"1Hello there!"},
		},
		{
			label:    "3 roundtrip, small payload",
			sessions: 1,
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label:    "1000 roundtrip, small payload",
			sessions: 1,
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
		{
			label:    "1 roundtrip, small payload, 10 sessions",
			sessions: 10,
			data:     []string{"1Hello there!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			client := newSBClient(t, tt.label,
				amqp.ConnMaxSessions(tt.sessions),
			)
			defer client.Close()

			for i := 0; i < tt.sessions; i++ {
				// Open a session
				session, err := client.NewSession()
				if err != nil {
					t.Fatal(err)
				}

				// Create a sender
				sender, err := session.NewSender(
					amqp.LinkTargetAddress(queueName),
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
						err = sender.Send(ctx, amqp.NewMessage([]byte(data)))
						cancel()
						if err != nil {
							sendErr = fmt.Errorf("Error after %d sends: %+v", i, err)
							return
						}
					}
				}()

				var receiveErr error
				go func() {
					defer wg.Done()

					// Create a receiver
					receiver, err := session.NewReceiver(
						amqp.LinkSourceAddress(queueName),
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
							receiveErr = fmt.Errorf("Error after %d receives: %+v", i, err)
							return
						}

						// Accept message
						msg.Accept()

						if !bytes.Equal([]byte(data), msg.GetData()) {
							receiveErr = fmt.Errorf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.GetData()))
						}
					}
				}()

				wg.Wait()

				if sendErr != nil || receiveErr != nil {
					t.Error("Send error:", sendErr)
					t.Fatal("Receive error:", receiveErr)
				}
			}

			client.Close() // close before leak check

			checkLeaks() // this is done here because queuesClient starts additional goroutines

			// Wait for Azure to update stats
			time.Sleep(1 * time.Second)

			q, err := queuesClient.Get(context.Background(), resourceGroup, namespace, queueName)
			if err != nil {
				t.Fatal(err)
			}

			if amc := *q.CountDetails.ActiveMessageCount; amc != 0 {
				t.Fatalf("Expected ActiveMessageCount to be 0, but it was %d", amc)
			}

			if dead := *q.CountDetails.DeadLetterMessageCount; dead > 0 {
				t.Fatalf("Expected DeadLetterMessageCount to be 0, but it was %d", dead)
			}
		})
	}
}

func TestIntegrationSend(t *testing.T) {
	queueName, queuesClient, cleanup := newTestQueue(t, "receive")
	defer cleanup()

	tests := []struct {
		label string
		data  []string
	}{
		{
			label: "3 send, small payload",
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			client := newSBClient(t, tt.label)
			defer client.Close()

			// Open a session
			session, err := client.NewSession()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			sender, err := session.NewSender(
				amqp.LinkTargetAddress(queueName),
			)
			if err != nil {
				t.Fatal(err)
			}

			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = sender.Send(ctx, amqp.NewMessage([]byte(data)))
				cancel()
				if err != nil {
					t.Fatalf("Error after %d sends: %+v", i, err)
					return
				}
			}
			sender.Close()
			client.Close() // close before leak check

			checkLeaks() // this is done here because queuesClient starts additional goroutines

			// Wait for Azure to update stats
			time.Sleep(1 * time.Second)

			q, err := queuesClient.Get(context.Background(), resourceGroup, namespace, queueName)
			if err != nil {
				t.Fatal(err)
			}

			if amc := *q.CountDetails.ActiveMessageCount; int(amc) != len(tt.data) {
				t.Fatalf("Expected ActiveMessageCount to be 0, but it was %d", amc)
			}

			if dead := *q.CountDetails.DeadLetterMessageCount; dead > 0 {
				t.Fatalf("Expected DeadLetterMessageCount to be 0, but it was %d", dead)
			}
		})
	}
}

func TestIntegrationClose(t *testing.T) {
	queueName, _, cleanup := newTestQueue(t, "close")
	defer cleanup()

	label := "link"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		client := newSBClient(t, label)
		defer client.Close()

		// Open a session
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		receiver, err := session.NewReceiver(
			amqp.LinkTargetAddress(queueName),
		)
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			err := receiver.Close()
			if err != nil {
				t.Fatalf("Expected nil error from receiver.Close(), got: %+v", err)
			}
		}()

		_, err = receiver.Receive(context.Background())
		if err != amqp.ErrLinkClosed {
			t.Fatalf("Expected ErrLinkClosed from receiver.Receiver, got: %+v", err)
			return
		}

		client.Close() // close before leak check

		checkLeaks()
	})

	label = "session"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		client := newSBClient(t, label)
		defer client.Close()

		// Open a session
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		receiver, err := session.NewReceiver(
			amqp.LinkTargetAddress(queueName),
		)
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			err := session.Close()
			if err != nil {
				t.Fatalf("Expected nil error from session.Close(), got: %+v", err)
			}
		}()

		_, err = receiver.Receive(context.Background())
		if err != amqp.ErrSessionClosed {
			t.Fatalf("Expected ErrSessionClosed from receiver.Receiver, got: %+v", err)
			return
		}

		client.Close() // close before leak check

		checkLeaks()
	})

	label = "conn"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		client := newSBClient(t, label)
		defer client.Close()

		// Open a session
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		receiver, err := session.NewReceiver(
			amqp.LinkTargetAddress(queueName),
		)
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			err := client.Close()
			if err != nil {
				t.Fatalf("Expected nil error from client.Close(), got: %+v", err)
			}
		}()

		_, err = receiver.Receive(context.Background())
		if err != amqp.ErrConnClosed {
			t.Fatalf("Expected ErrConnClosed from receiver.Receiver, got: %+v", err)
			return
		}

		checkLeaks()
	})
}

func TestIntegration_EventHubs_RoundTrip(t *testing.T) {
	hubName, _, cleanup := newTestHub(t, "receive")
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
			label: "1 roundtrip, medium payload",
			data:  []string{strings.Repeat("Hello", 1000/len("Hello"))},
		},
		{
			label: "1 roundtrip, large payload",
			data:  []string{strings.Repeat("H", 133793)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			client := newEHClient(t, tt.label)

			// Open a session
			session, err := client.NewSession()
			if err != nil {
				t.Fatal(err)
			}

			// Create receivers before sending to simplify offsets
			receive := createEventHubReceivers(t, hubName, session, len(tt.data))

			// Create a sender
			sender, err := session.NewSender(
				amqp.LinkTargetAddress(hubName),
			)
			if err != nil {
				t.Fatalf("%+v\n", err)
			}

			// Send messages
			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = sender.Send(ctx, amqp.NewMessage([]byte(data)))
				cancel()
				if err != nil {
					t.Fatalf("Error after %d sends: %+v", i, err)
				}
			}

			// Receive from all partitions
			received, errs := receive()

			// check total number of messages received
			var total int
			for _, count := range received {
				total += count
			}
			if total != len(tt.data) {
				t.Errorf("Expected %d messages, got %d", len(tt.data), total)
				for i, err := range errs {
					if err != nil {
						t.Errorf("Partition %d got error: %+v", i, err)
					}
				}
			}

			// check that data matches
			for _, data := range tt.data {
				count, ok := received[data]
				if !ok {
					t.Errorf("Expected to receive message %q but didn't", data)
					continue
				}
				if count > 1 {
					received[data]--
				} else {
					delete(received, data)
				}
			}

			// report any unexpected messages
			for msg, count := range received {
				t.Errorf("Received %d extra messages: %q", count, msg)
			}

			client.Close() // close before leak check

			checkLeaks()
		})
	}
}

func createEventHubReceivers(t testing.TB, hubName string, session *amqp.Session, expectedMessages int) func() (map[string]int, []error) {
	// Create a receivers on both partitions
	var receivers []*amqp.Receiver
	for i := 0; i < 2; i++ {
		receiver, err := session.NewReceiver(
			amqp.LinkSourceAddress(hubName+"/ConsumerGroups/$default/Partitions/"+strconv.Itoa(i)),
			amqp.LinkSelectorFilter("amqp.annotation.x-opt-offset > '@latest'"),
			amqp.LinkCredit(10),
			amqp.LinkBatching(false),
		)
		if err != nil {
			t.Fatalf("Error creating receiver: %v", err)
		}
		receivers = append(receivers, receiver)
	}

	return func() (map[string]int, []error) {
		var (
			received  = make(map[string]int)
			errs      = make([]error, len(receivers))
			wg        sync.WaitGroup
			remaining = int32(expectedMessages)
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		// receive from each partition concurrently
		for rxIdx, receiver := range receivers {
			wg.Add(1)
			go func(rxIdx int, receiver *amqp.Receiver) {
				defer wg.Done()
				defer receiver.Close()

				for i := 0; ; i++ {
					msg, err := receiver.Receive(ctx)
					if err != nil {
						errs[rxIdx] = fmt.Errorf("Error after %d receives: %+v", i, err)
						return
					}

					// Accept message
					msg.Accept()

					received[string(msg.GetData())]++

					// cancel all receivers once expected messages have been received
					if atomic.AddInt32(&remaining, -1) < 1 {
						cancel()
						return
					}
				}
			}(rxIdx, receiver)
		}
		wg.Wait()

		return received, errs
	}
}

func TestIssue48_ReceiverModeSecond(t *testing.T) {
	azDescription := regexp.MustCompile(`The format code '0x68' at frame buffer offset '\d+' is invalid or unexpected`)

	hubName, _, cleanup := newTestHub(t, "issue48")
	defer cleanup()

	label := "issue48"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		client := newEHClient(t, "issue48")
		defer client.Close()

		// Open a session
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		sender, err := session.NewSender(
			amqp.LinkTargetAddress(hubName),
		)
		if err != nil {
			t.Fatalf("%+v\n", err)
		}

		// First send should succeed
		err = sender.Send(context.Background(), &amqp.Message{
			Format: 0x80013700,
			Data: [][]byte{
				[]byte("hello"),
				[]byte("there"),
			},
		})
		time.Sleep(1 * time.Second) // Have to wait long enough for disposition to come through.
		if err != nil {
			t.Fatalf("Unexpected error response: %+v", err)
		}

		// Second send should get async error
		err = sender.Send(context.Background(), &amqp.Message{
			Data: [][]byte{
				[]byte("hello"),
			},
		})
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if err, ok := err.(*amqp.Error); !ok || !azDescription.MatchString(err.Description) {
			t.Fatalf("Unexpected error response: %+v", err)
		}

		client.Close() // close before leak check

		checkLeaks()
	})

	label = "issue48-receiver-mode-second"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		client := newEHClient(t, "issue48")
		defer client.Close()

		// Open a session
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		sender, err := session.NewSender(
			amqp.LinkTargetAddress(hubName),
			amqp.LinkReceiverSettle(amqp.ModeSecond),
		)
		if err != nil {
			t.Fatalf("%+v\n", err)
		}

		err = sender.Send(context.Background(), &amqp.Message{
			Format: 0x80013700,
			Data: [][]byte{
				[]byte("hello"),
				[]byte("there"),
			},
		})
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if err, ok := err.(*amqp.Error); !ok || !azDescription.MatchString(err.Description) {
			t.Fatalf("Unexpected error response: %+v", err)
		}

		client.Close() // close before leak check

		checkLeaks()
	})
}

func dump(i interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	enc.Encode(i)
}

func newEHClient(t testing.TB, label string, opts ...amqp.ConnOption) *amqp.Client {
	return newClient(t, label, ehNamespace, ehAccessKeyName, ehAccessKey, opts...)
}

func newSBClient(t testing.TB, label string, opts ...amqp.ConnOption) *amqp.Client {
	return newClient(t, label, namespace, accessKeyName, accessKey, opts...)
}

func newClient(t testing.TB, label, ns, username, password string, opts ...amqp.ConnOption) *amqp.Client {
	opts = append(opts,
		amqp.ConnSASLPlain(username, password),
	)

	if *tlsKeyLog == "" && *recordDir == "" {
		client, err := amqp.Dial("amqps://"+ns+".servicebus.windows.net", opts...)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}

	var tlsConfig tls.Config

	if *tlsKeyLog != "" {
		f, err := os.OpenFile(*tlsKeyLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}

		tlsConfig = tls.Config{
			KeyLogWriter: f,
		}
	}

	var conn net.Conn
	conn, err := tls.Dial("tcp", ns+".servicebus.windows.net:5671", &tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

	if *recordDir != "" {
		path := filepath.Join(*recordDir, label)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		conn = testconn.NewRecorder(f, conn)
	}

	opts = append(opts,
		amqp.ConnServerHostname(ns+".servicebus.windows.net"),
	)
	client, err := amqp.New(conn, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newTestQueue(tb testing.TB, suffix string) (string, servicebus.QueuesClient, func()) {
	shouldRunIntegration(tb)

	queueName := "integration-" + suffix + "-" + strconv.FormatUint(rand.Uint64(), 10)
	tb.Log("Creating queue", queueName)

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
	_, err = queuesClient.CreateOrUpdate(context.Background(), resourceGroup, namespace, queueName, params)
	if err != nil {
		tb.Fatal(err)
	}

	cleanup := func() {
		_, err = queuesClient.Delete(context.Background(), resourceGroup, namespace, queueName)
		if err != nil {
			tb.Logf("Unable to remove queue: %s - %v", queueName, err)
		}
	}

	return queueName, queuesClient, cleanup
}

func newTestHub(tb testing.TB, suffix string) (string, eventhub.EventHubsClient, func()) {
	shouldRunIntegration(tb)

	hubName := "integration-" + suffix + "-" + strconv.FormatUint(rand.Uint64(), 10)
	tb.Log("Creating hub", hubName)

	oauthConfig, err := adal.NewOAuthConfig(azure.PublicCloud.ActiveDirectoryEndpoint, tenantID)
	if err != nil {
		tb.Fatal(err)
	}
	token, err := adal.NewServicePrincipalToken(*oauthConfig, clientID, clientSecret, azure.PublicCloud.ResourceManagerEndpoint)
	if err != nil {
		tb.Fatal(err)
	}

	ehClient := eventhub.NewEventHubsClient(subscriptionID)
	ehClient.Authorizer = autorest.NewBearerAuthorizer(token)

	params := eventhub.Model{
		Properties: &eventhub.Properties{
			PartitionCount:         ptrInt64(2),
			MessageRetentionInDays: ptrInt64(1), // required on basic
		},
	}
	_, err = ehClient.CreateOrUpdate(context.Background(), resourceGroup, ehNamespace, hubName, params)
	if err != nil {
		tb.Fatal(err)
	}

	cleanup := func() {
		_, err = ehClient.Delete(context.Background(), resourceGroup, ehNamespace, hubName)
		if err != nil {
			tb.Logf("Unable to remove queue: %s - %v", hubName, err)
		}
	}

	return hubName, ehClient, cleanup
}

func ptrInt64(i int64) *int64 {
	return &i
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" && !isForkPR {
		panic("Environment variable '" + key + "' required for integration tests.")
	}
	return v
}

func shouldRunIntegration(tb testing.TB) {
	if isForkPR {
		tb.Skip("skipping integration test in PR")
	}
}

func repeatStrings(count int, strs ...string) []string {
	var out []string
	for i := 0; i < count; i += len(strs) {
		out = append(out, strs...)
	}
	return out[:count]
}
