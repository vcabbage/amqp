// +build !debug

package amqp

// dummy functions used when debugging is not enabled

func debug(_ int, _ string, _ ...interface{})      {}
func debugFrame(c *conn, prefix string, fr *frame) {}
