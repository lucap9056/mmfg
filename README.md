# MMFG

MMFG is a high-performance Inter-Process Communication (IPC) framework built around zero-copy data transfer and extremely high concurrency requirements. It uses shared memory to move data between processes without copying, and Unix Domain Sockets (UDS) for efficient signal delivery.

MMFG is primarily used to achieve **zero-copy chained processing** among multiple independent processes (Nodes). Through a "handoff" mechanism, data ownership can be transferred between nodes without any memory copying or reallocation, making it ideal for building high-performance data processing pipelines.

## Key Features

- **Zero-Copy Chained Processing**: The core advantage of MMFG. Data ownership can be transferred directly between nodes (`Next("NodeB")`) without releasing or reallocating memory, ensuring a completely zero-copy process.
- **Hub-and-Node Architecture**: A central Hub manages resources and coordinates multiple passive Nodes to perform tasks.
- **Low-Latency Signal Delivery**:
  - **Hub -> Node**: Each node has a dedicated `eventfd` for precise and low-latency waking, avoiding the "thundering herd" problem.
  - **Node -> Hub**: Uses a shared `eventfd` combined with a high-performance MPSC (Multi-Producer, Single-Consumer) ring buffer located in shared memory.
- **Dynamic Resource Management**:
  - **Chunks & Blocks**: Memory is managed in 4MB Chunks, further subdivided into 4KB Blocks.
  - **Auto-Scaling**: Supports dynamic expansion of Stripes to meet data growth demands.
- **Cross-Language Support**: Implementation provided for both Go and Rust.
- **Lock-Free Design**: Synchronization achieved through status bits and CAS (Compare-And-Swap) operations.

## Chained Processing (Handoff) Protocol

To keep complex workflows zero-copy end-to-end, MMFG implements an atomic handoff protocol:

1. **Lazy Binding**: Connections start as local resources of the Hub. A global `SlotID` is only assigned upon the first data transfer to a Node.
2. **State Synchronization**: The Hub ensures the previous owner has completed its task (status is Done) before migrating access rights.
3. **Synchronous Handoff**: The `Next(nodeName)` method transfers ownership and **blocks** until the target node has finished processing, after which control (and updated metadata) is handed back to the Hub.
4. **Pipeline Efficiency**: Data flows from Node A to Node B to Node C, remaining in shared memory throughout the process with absolutely no copying.

## System Architecture

### Hub (Resource Manager)
The central controller of the system.
- Manages the Node connection pool.
- Allocates and manages the shared memory bus.
- Coordinates task dispatching and resource lifecycles.

### Node (Passive Responder)
Endpoints responsible for processing tasks assigned by the Hub.
- Listens for Hub connections via UDS.
- Directly operates on shared memory blocks.
- Reports task completion to the Hub via a shared response queue.

### Bus and Memory Layout
- **Chunk**: A 4MB memory unit created via `memfd_create`.
- **Block**: The smallest logical unit for resource allocation, sized at 4KB.
- **Stripe**: A sequence of Blocks. The first Block is the **Header Block**, storing metadata such as data length and the Block sequence.

## Usage Example (Go)

### Hub Side (Chained Processing)

```go
// 1. Initialize Hub
h, err := hub.NewHub()
if err != nil {
    log.Fatal(err)
}

// 2. Request a local connection (e.g., initial capacity of 2 Blocks)
// SlotID is not consumed at this stage, until the first call to Next()
conn, err := h.Request(ctx, 2, false)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// 3. Hub writes initial data locally
conn.Write([]byte("initial data"))

// 4. Transfer to Node_A and wait for it to finish processing
// This operation automatically binds the SlotID and notifies Node_A
if err := conn.Next("Node_A"); err != nil {
    log.Fatal(err)
}

// 5. Transfer to Node_B and wait for it to finish processing
// Data processed by Node_A is zero-copy visible to Node_B
if err := conn.Next("Node_B"); err != nil {
    log.Fatal(err)
}

// 6. Hub reads the final result
result := make([]byte, 1024)
n, _ := conn.Read(result)
fmt.Printf("Final result: %s\n", string(result[:n]))
```
