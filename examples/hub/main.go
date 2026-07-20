//go:build unix

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
)

func main() {
	// 1. Initialize Hub
	h, err := hub.NewHub()
	if err != nil {
		log.Fatalf("Failed to create hub: %v", err)
	}

	// 2. Dial to a specific Node
	nodeSocket := "/tmp/mmfg_node.sock"
	h.Dial("my_node", nodeSocket)

	// 3. Request connection (local resource first, no slot assigned yet)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := h.Request(ctx, 1, false)
	if err != nil {
		log.Fatalf("Failed to request local connection: %v", err)
	}
	defer conn.Close()

	// 4. Send data locally
	data := []byte("Hello MMFG!")
	if _, err := conn.Write(data); err != nil {
		log.Fatalf("Failed to write locally: %v", err)
	}

	// 5. Transfer to Node (Handoff and wait for processing)
	if err := conn.Next("my_node"); err != nil {
		log.Fatalf("Handoff to my_node failed: %v", err)
	}

	fmt.Println("Handoff successful, communication completed.")
}
