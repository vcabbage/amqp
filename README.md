# **pack.ag/amqp**

[![Go Report Card](https://goreportcard.com/badge/pack.ag/amqp)](https://goreportcard.com/report/pack.ag/amqp)
[![Coverage Status](https://coveralls.io/repos/github/vcabbage/amqp/badge.svg?branch=master)](https://coveralls.io/github/vcabbage/amqp?branch=master)
[![Build Status](https://travis-ci.org/vcabbage/amqp.svg?branch=master)](https://travis-ci.org/vcabbage/amqp)
[![Build status](https://ci.appveyor.com/api/projects/status/to267eqa7nojpv56?svg=true)](https://ci.appveyor.com/project/vCabbage/amqp)
[![GoDoc](https://godoc.org/pack.ag/amqp?status.svg)](http://godoc.org/pack.ag/amqp)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/vcabbage/amqp/master/LICENSE)


pack.ag/amqp is an AMQP 1.0 client implementation for Go.

AMQP 1.0 is not compatible with AMQP 0-9-1 or 0-10, which are
the most common AMQP protocols in use today.

This project is currently alpha status, though is currently being used by my employer
in a pre-production capacity. The current focus is reading from
Microsoft Azure's Service Bus. Only receive operations have been implemented.
Producer operations will be implemented later.

API is subject to change until 1.0.0. If you choose to use this library, please vendor it.

---

## Install

```
go get -u pack.ag/amqp
```

## Example Usage

``` go
package mypackage

import (
	"context"
	"fmt"
	"log"

	"pack.ag/amqp"
)

func main() {
	// Create client
	client, err := amqp.Dial("amqps://my-namespace.servicebus.windows.net",
		amqp.ConnSASLPlain("access-key-name", "access-key"),
	)
	if err != nil {
		log.Fatal("Dialing AMQP server:", err)
	}
	defer client.Close()

	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
	}

	// Create a receiver
	receiver, err := session.NewReceiver(
		amqp.LinkSource("/queue-name"),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		// Receive next message
		msg, err := receiver.Receive(ctx)
		if err != nil {
			log.Fatal("Reading message from AMQP:", err)
		}

		// Accept message
		msg.Accept()

		fmt.Printf("Message received: %s\n", msg.Data)
	}
}
```

---

### Notable Bugs/Shortcomings

- [ ] Closing a sessions does not send an end performative.
- [ ] Testing is lacking. Only fuzz testing is currently being performed.

### Features - Short Term

- [ ] Set sender filters to support Azure Event Hubs.

### Features - Medium Term

- [ ] Support message producer operations.

### Other Notes

By default, this package depends only on the standard library. Building with the
`pkgerrors` tag will cause errors to be created/wrapped by the github.com/pkg/errors
library. This can be useful for debugging and when used in a project using
github.com/pkg/errors.
