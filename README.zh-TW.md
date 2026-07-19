# MMFG (Memory Mapped Fast Gateway)

MMFG 是一個高效能的 process 間通訊 (IPC) 框架，圍繞零拷貝 (zero-copy) 資料傳輸與極高併發需求而設計。它利用共享記憶體在 process 之間搬移資料而不需拷貝，並透過 Unix Domain Sockets (UDS) 實現高效的信號傳遞。

MMFG 的主要用途在於實現多個獨立 process (Nodes) 之間的**零拷貝鏈式處理 (chained processing)**。透過「接管 (handoff)」機制，資料所有權可以在節點間轉移，而無需進行任何記憶體拷貝或重新分配，非常適合建構高效能的資料處理流水線 (data pipelines)。

## 核心特性

- **零拷貝鏈式處理**：MMFG 的核心優勢。資料所有權可以在節點間直接轉移 (`Next("NodeB")`)，無需釋放或重新分配記憶體，確保整個處理流程達到完全零拷貝。
- **Hub-and-Node 架構**：由一個中心的 Hub 負責管理資源，並協調多個被動 Node 執行任務。
- **低延遲信號傳遞**：
  - **Hub -> Node**：每個節點擁有專屬的 `eventfd`，實現精準且低延遲的喚醒，避免「驚群效應 (thundering herd)」。
  - **Node -> Hub**：共用 `eventfd` 並結合位於共享記憶體中的高效能 MPSC (多生產者、單消費者) 環形佇列 (Ring Buffer)。
- **動態資源管理**：
  - **Chunks & Blocks**：記憶體以 4MB 的 Chunk 為單位進行管理，並細分為 4KB 的 Block。
  - **自動擴展**：支援 Stripe 的動態擴展，以滿足資料增長的需求。
- **跨語言支援**：提供 Go 與 Rust 的實作版本。
- **無鎖化設計 (Lock-Free)**：透過狀態位元與 CAS (Compare-And-Swap) 操作實現同步。

## 鏈式處理 (Handoff) 協定

為了在複雜的工作流中維持全程零拷貝，MMFG 實作了原子化的接管協定：

1. **延遲綁定 (Lazy Binding)**：連線最初作為 Hub 的本地資源。只有在第一次將資料轉移給 Node 時，才會分配全域 `SlotID`。
2. **狀態同步**：Hub 確保前一個持有者已完成任務 (狀態為 Done)，才進行存取權遷移。
3. **同步接管 (Synchronous Handoff)**：`Next(nodeName)` 方法會轉移所有權並**阻塞**直到目標節點處理完畢，隨後將控制權（及更新後的 metadata）交還給 Hub。
4. **流水線效率**：資料從 Node A 流向 Node B 再到 Node C，全程留在共享記憶體中，完全無需拷貝。

## 系統架構

### Hub (資源管理者)
系統的中央 controller。
- 管理 Node 連線池。
- 分配並管理共享記憶體 bus。
- 協調任務分派與資源生命週期。

### Node (被動回應者)
負責處理 Hub 交辦任務的 endpoint。
- 透過 UDS 監聽 Hub 連線。
- 直接操作共享記憶體區塊。
- 透過共用回應 queue 向 Hub 回報任務完成。

### Bus 與記憶體佈局
- **Chunk**：透過 `memfd_create` 建立的 4MB 記憶體單位。
- **Block**：資源分配的最小邏輯單位，大小為 4KB。
- **Stripe**：一組 Block 的序列。第一個 Block 為 **Header Block**，存放資料長度與 Block 序列等 metadata。

## 使用範例 (Go)

### Hub 端 (鏈式處理)

```go
// 1. 初始化 Hub
h, err := hub.NewHub()
if err != nil {
    log.Fatal(err)
}

// 2. 請求一個本地連線 (例如 2 個 Block 的初始容量)
// 此時尚未消耗 SlotID，直到第一次呼叫 Next()
conn, err := h.Request(ctx, 2, false)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// 3. Hub 在本地寫入初始資料
conn.Write([]byte("初始資料"))

// 4. 轉移至 Node_A 並等待其處理完成
// 此操作會自動綁定 SlotID 並通知 Node_A
if err := conn.Next("Node_A"); err != nil {
    log.Fatal(err)
}

// 5. 轉移至 Node_B 並等待其處理完成
// Node_A 處理過的資料對 Node_B 是零拷貝可見的
if err := conn.Next("Node_B"); err != nil {
    log.Fatal(err)
}

// 6. Hub 讀取最終結果
result := make([]byte, 1024)
n, _ := conn.Read(result)
fmt.Printf("最終結果: %s\n", string(result[:n]))
```

