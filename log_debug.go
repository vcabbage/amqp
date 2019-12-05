// +build debug

package amqp

import "log"
import "os"
import "strconv"

var (
	debugLevel = 1
	logger     = log.New(os.Stderr, "", log.Lmicroseconds)
)

func init() {
	level, err := strconv.Atoi(os.Getenv("DEBUG_LEVEL"))
	if err != nil {
		return
	}

	debugLevel = level
}

func debug(level int, format string, v ...interface{}) {
	if level <= debugLevel {
		logger.Printf(format, v...)
	}
}

func debugFrame(c *conn, prefix string, fr *frame) {
	if debugLevel == 0 { // Fast exit for no logging
		return
	}
	// Set level by frame type
	level := 1 // Normal frames at 1
	switch fr.body.(type) {
	case *performTransfer: // High-volume messages
		level = 2
	case *performFlow, *performDisposition: // Noisy flow and acknowledgment
		level = 3
	}
	if level <= debugLevel {
		logger.Printf("%p %s[%d]: %s", c, prefix, fr.channel, fr.body)
	}
}
