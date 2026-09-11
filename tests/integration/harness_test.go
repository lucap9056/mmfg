//go:build linux

package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
	"github.com/nautrouds/mmfg/v2/go/node"
)

const (
	sockRust1 = "/tmp/mmfg_it_rust1.sock"
	sockRust2 = "/tmp/mmfg_it_rust2.sock"
	sockGoA   = "/tmp/mmfg_it_go_a.sock"
	sockGoB   = "/tmp/mmfg_it_go_b.sock"
)

var (
	testHub     *hub.Hub
	rustBinPath string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "getwd:", err)
		return 1
	}
	rustBinPath = filepath.Join(wd, "..", "..", "rust", "target", "release", "node")
	if _, err := os.Stat(rustBinPath); err != nil {
		fmt.Fprintf(os.Stderr, "rust node binary missing at %s (run `cargo build --release` in rust/ first): %v\n", rustBinPath, err)
		return 1
	}

	rust1, err := startRustNode(sockRust1)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start rust1:", err)
		return 1
	}
	defer stopRustNode(rust1)

	rust2, err := startRustNode(sockRust2)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start rust2:", err)
		return 1
	}
	defer stopRustNode(rust2)

	goA := node.NewNode(node.WithHandler(echoWithTag("A")))
	go goA.Listen(sockGoA)
	goB := node.NewNode(node.WithHandler(echoWithTag("B")))
	go goB.Listen(sockGoB)

	if err := waitForSocket(sockGoA, 2*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := waitForSocket(sockGoB, 2*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	h, err := hub.NewHub()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub:", err)
		return 1
	}
	for name, sock := range map[string]string{
		"rust1": sockRust1,
		"rust2": sockRust2,
		"goA":   sockGoA,
		"goB":   sockGoB,
	} {
		if err := h.Dial(name, sock); err != nil {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", name, err)
			return 1
		}
	}

	testHub = h
	return m.Run()
}

func echoWithTag(tag string) node.Handler {
	suffix := []byte("-" + tag)
	return func(conn node.Connection) {
		data, err := io.ReadAll(conn)
		if err != nil && err != io.EOF {
			return
		}
		conn.Write(append(data, suffix...))
	}
}

type rustNode struct {
	cmd *exec.Cmd
}

func startRustNode(sockPath string) (*rustNode, error) {
	os.Remove(sockPath)

	cmd := exec.Command(rustBinPath, sockPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "READY" {
				close(ready)
				break
			}
		}
		_ = scanner.Err()
		io.Copy(io.Discard, stdout)
	}()

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		return nil, fmt.Errorf("rust node at %s did not print READY in time", sockPath)
	}

	if err := waitForSocket(sockPath, 2*time.Second); err != nil {
		cmd.Process.Kill()
		return nil, err
	}

	return &rustNode{cmd: cmd}, nil
}

func stopRustNode(n *rustNode) {
	if n == nil || n.cmd.Process == nil {
		return
	}
	n.cmd.Process.Kill()
	n.cmd.Wait()
}

func waitForSocket(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for socket %s to accept connections", path)
}

func request(t *testing.T) hub.Connection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := testHub.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	return conn
}

func randomBytes(n int, seed int64) []byte {
	buf := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(buf)
	return buf
}
