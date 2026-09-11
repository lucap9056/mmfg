//go:build linux

package main

import (
	"io"
	"log"
	"os"

	"github.com/nautrouds/mmfg/v2/go/node"
)

func main() {
	socketPath := "/tmp/mmfg_node.sock"
	os.Remove(socketPath)

	// 1. Define Node handler
	handler := func(conn node.Connection) {
		log.Println("Node: Received new connection for processing")

		// Read data
		data, err := io.ReadAll(conn)
		if err != nil && err != io.EOF {
			log.Printf("Node: Read error: %v", err)
			return
		}

		log.Printf("Node: Processed data: %s", string(data))

		// Write response
		if _, err := conn.Write([]byte("Acknowledged")); err != nil {
			log.Printf("Node: Write error: %v", err)
		}
	}

	// 2. Start Node
	n := node.NewNode(node.WithHandler(handler))
	log.Printf("Node listening on %s", socketPath)
	if err := n.Listen(socketPath); err != nil {
		log.Fatalf("Node: Listen error: %v", err)
	}
}
