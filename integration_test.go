// +build integration

package amqp_test

import (
	"bytes"
	"context"
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

func TestIntegrationReceive(t *testing.T) {
	queueName, cleanup := newTestQueue(t, "receive")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	data := []byte("Hello there!")
	err = sender.Send(ctx, &amqp.Message{
		Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a receiver
	receiver, err := session.NewReceiver(
		amqp.LinkSource(queueName),
		amqp.LinkCredit(10),
	)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Accept message
	msg.Accept()

	if !bytes.Equal(data, msg.Data) {
		t.Errorf("Expected received message to be %v, but it was %v", string(data), string(msg.Data))
	}
}

func newTestQueue(tb testing.TB, suffix string) (string, func()) {
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

	return queueName, cleanup
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("Environment variable '" + key + "' required for integration tests.")
	}
	return v
}
