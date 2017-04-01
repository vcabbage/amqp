package main

import (
	"fmt"
	"os"

	"time"

	"pack.ag/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://10.211.55.4:5672/", amqp.OptSASLPlain("guest", "guest"))
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connection established w/ MaxFrameSize: %d, ChannelMax: %d.\n", conn.MaxFrameSize(), conn.ChannelMax())

	_, err = conn.Session()
	if err != nil {
		fmt.Println(err)
	}

	_, err = conn.Session()
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(1 * time.Second)

}
