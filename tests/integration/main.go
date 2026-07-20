//go:build unix

package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
)

func main() {

	wd, _ := os.Getwd()

	binPath := filepath.Join(wd, "rust", "target", "release", "node")
	sockPath := "/tmp/mmfg_rust_integration.sock"

	os.Remove(sockPath)

	cmd := exec.Command(binPath, sockPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start Rust Node: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(1 * time.Second)

	h, err := hub.NewHub()
	if err != nil {
		log.Fatalf("Hub err: %v", err)
	}
	if err := h.Dial("rust_node", sockPath); err != nil {
		log.Fatalf("Dial err at %s: %v", sockPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		log.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	payload := []byte("hello-from-go")
	if _, err := conn.Write(payload); err != nil {
		log.Fatalf("Write failed: %v", err)
	}

	if err := conn.Next("rust_node"); err != nil {
		log.Fatalf("Next(rust_node) failed: %v", err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}

	res := string(buf[:n])
	if res != "hello-from-go" {
		log.Fatalf("Mismatch: got %s, want %s", res, "hello-from-go")
	}
	log.Println("Basic integration test passed!")

	log.Println("--- Starting Large Data Transfer Test ---")
	testMegaExpansion(conn)

	log.Println("Cross-language integration tests passed!")
}

func testMegaExpansion(conn hub.Connection) {
	megaData := make([]byte, 2*1024*1024) // 2MB
	for i := range megaData {
		megaData[i] = byte(i % 256)
	}

	log.Printf("Writing %d bytes to Rust Node...", len(megaData))
	if _, err := conn.Write(megaData); err != nil {
		log.Fatalf("Mega write failed: %v", err)
	}

	if err := conn.Next("rust_node"); err != nil {
		log.Fatalf("Next(rust_node) failed: %v", err)
	}

	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if !bytes.Equal(buf[:n], megaData[:n]) {
		log.Fatal("Mega data head mismatch")
	}
	log.Println("Mega write expansion test passed!")
}
