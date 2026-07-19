//go:build unix

package hub

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/nautrouds/mmfg/go/shm"
	mmfg_sync "github.com/nautrouds/mmfg/go/sync"
	"github.com/nautrouds/mmfg/internal/netutil"
)

// Hub is the central controller.
type Hub struct {
	mu      sync.RWMutex
	bus     *shm.Bus
	ctrl    *shm.Control
	respEv  *mmfg_sync.Eventfd
	nodes   map[string]*NodeInfo
	waiters [shm.MaxTotalSlots]atomic.Pointer[chan error]
	nodeIDs [shm.MaxNodes]uint64

	chunk0Name string
	limit      chan struct{}
}

type Option func(*Hub)

// WithChunk0Name sets the name of the initial memory-mapped chunk.
func WithChunk0Name(name string) Option {
	return func(h *Hub) {
		h.chunk0Name = name
	}
}

type NodeInfo struct {
	nodeID int
	conn   *net.UnixConn
	nodeEv *mmfg_sync.Eventfd
}

func (h *Hub) allocNodeID() (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 1; i < shm.MaxNodes; i++ {
		idx := i / 64
		bit := uint(i % 64)
		if (h.nodeIDs[idx] & (1 << bit)) == 0 {
			h.nodeIDs[idx] |= (1 << bit)
			return i, nil
		}
	}
	return 0, fmt.Errorf("max nodes reached")
}

func (h *Hub) freeNodeID(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx := id / 64
	bit := uint(id % 64)
	h.nodeIDs[idx] &= ^(1 << bit)
}

func NewHub(opts ...Option) (*Hub, error) {

	h := &Hub{
		nodes:      make(map[string]*NodeInfo),
		chunk0Name: "mmfg_chunk0",
		limit:      make(chan struct{}, shm.MaxTotalSlots),
	}

	chunk0, err := shm.NewChunk(h.chunk0Name)
	if err != nil {
		return nil, err
	}

	bus, err := shm.NewBus(chunk0, h.AppendedChunkBroacast)
	if err != nil {
		return nil, err
	}

	h.bus = bus

	for _, opt := range opts {
		opt(h)
	}

	// Allocate Control Stripe (1MB)
	ctrlStripe, err := h.bus.AllocStripe(shm.ControlStripeSize / shm.BlockSize)
	if err != nil {
		chunk0.Close()
		return nil, err
	}

	h.ctrl = shm.NewControl(ctrlStripe)
	h.ctrl.Init()

	h.respEv, err = mmfg_sync.NewEventfd()
	if err != nil {
		chunk0.Close()
		return nil, err
	}

	go h.dispatchLoop()
	return h, nil
}

func (h *Hub) AppendedChunkBroacast(chunk *shm.Chunk) {
	for _, node := range h.nodes {
		h.syncChunk(node, chunk.Fd)
	}
}

func (h *Hub) allocSlot(stripe *shm.Stripe) (uint32, error) {
	h.limit <- struct{}{}
	slotID, ok := h.ctrl.AllocSlot()
	if !ok {
		<-h.limit
		return 0, fmt.Errorf("no slots available")
	}

	cid, off := stripe.Sequence[0], stripe.Sequence[1]
	h.ctrl.SetStripe(slotID, shm.StripeStatusReady, cid, off, 0)
	return slotID, nil
}

func (h *Hub) submit(node *NodeInfo, slotID uint32) error {
	reqQOff := shm.GetNodeReqQueueOffset(node.nodeID)

	if !h.ctrl.Push(reqQOff, slotID, shm.CMD_PROCESS) {
		return fmt.Errorf("request queue full")
	}

	return node.nodeEv.Notify()
}

func (h *Hub) dispatchLoop() error {
	respQOff := uintptr(shm.OFF_RESP_QUEUE)
	_ = shm.GetNodeReqQueueOffset(0) // Hub uses 0

	for {

		slotID, cmd, ok := h.ctrl.Pop(respQOff)
		if ok {
			switch cmd {
			case shm.CMD_PROCESS:

				chPtr := h.waiters[slotID].Swap(nil)
				if chPtr != nil {
					*chPtr <- nil
				}
			case shm.CMD_REQUEST_EXPAND:
				owner := h.ctrl.GetStripeOwner(slotID)
				go h.handleExpandRequest(int(owner), slotID)
			}
			continue
		}

		if err := h.respEv.Wait(); err != nil {
			return fmt.Errorf("dispatch loop stopping: %v", err)
		}

	}

}

func (h *Hub) Dial(nodeName string, socketPath string) error {
	conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		return err
	}

	nodeID, err := h.allocNodeID()
	if err != nil {
		conn.Close()
		return err
	}

	nodeEv, err := mmfg_sync.NewEventfd()
	if err != nil {
		conn.Close()
		return err
	}

	header := append(
		mmfg_sync.CONN_HEADER,
		[]byte{
			mmfg_sync.CONN_VERSION,
			byte(netutil.MsgHandshake),
			byte(nodeID),
		}...,
	)

	h.mu.RLock()
	chunkCount := h.bus.ChunkCount()
	fds := make([]int, chunkCount+2)

	fds[0] = nodeEv.Fd()
	fds[1] = h.respEv.Fd()

	for i := range chunkCount {
		fds[i+2] = h.bus.GetChunk(int16(^i)).Fd
	}
	h.mu.RUnlock()

	if err := netutil.SendMsgWithFDs(conn, header, fds...); err != nil {
		return err
	}

	nodeInfo := &NodeInfo{
		nodeID: nodeID,
		conn:   conn,
		nodeEv: nodeEv,
	}

	h.mu.Lock()
	afterChunkCount := h.bus.ChunkCount()
	if chunkCount != afterChunkCount && afterChunkCount > chunkCount {
		fds := make([]int, afterChunkCount-chunkCount)
		for i := range fds {
			fds[i] = h.bus.GetChunk(int16(^(i + chunkCount))).Fd
		}
		msg := []byte{byte(netutil.MsgNewChunk)}
		if err := netutil.SendMsgWithFDs(conn, msg, fds...); err != nil {
			h.mu.Unlock()
			return err
		}
	}
	h.nodes[nodeName] = nodeInfo
	h.mu.Unlock()

	go h.watchdog(nodeName, nodeInfo)

	return nil
}

func (h *Hub) watchdog(name string, node *NodeInfo) {
	buf := make([]byte, 1)
	for {
		_, err := node.conn.Read(buf)
		if err != nil {
			h.CleanupResources(node.nodeID)
			h.freeNodeID(node.nodeID)
			h.mu.Lock()
			delete(h.nodes, name)
			h.mu.Unlock()
			return
		}
	}
}

func (h *Hub) CleanupResources(nodeID int) {
	for i := uint32(0); i < shm.MaxTotalSlots; i++ {
		if h.ctrl.GetStripeStatus(i) != shm.StripeStatusIdle {
			owner := h.ctrl.GetStripeOwner(i)
			if int(owner) == nodeID {
				cid, off := h.ctrl.GetStripeHeader(i)
				headerCk := h.bus.GetChunk(cid)
				headerOff := int(off) * shm.BlockSize
				headerBlk := headerCk.Data[headerOff : headerOff+shm.BlockSize]
				_, _, seq := shm.DecodeHeader(headerBlk)
				var chunkID int16
				for _, id := range seq {
					if id < 0 {
						chunkID = id
						continue
					}
					h.bus.FreeBlock(chunkID, id)
				}

				h.ctrl.FreeSlot(i)

			}
		}
	}
}

func (h *Hub) syncChunk(node *NodeInfo, fds ...int) {
	header := []byte{byte(netutil.MsgNewChunk)}
	err := netutil.SendMsgWithFDs(node.conn, header, fds...)
	if err != nil {
		return
	}
}

func (h *Hub) handleExpandRequest(nodeID int, slotID uint32) {

	cid, off := h.ctrl.GetStripeHeader(slotID)

	h.mu.RLock()
	chunk := h.bus.GetChunk(cid)
	h.mu.RUnlock()

	offset := int(off) * shm.BlockSize
	headerBlk := chunk.Data[offset : offset+shm.BlockSize]

	dataLen, blkCnt, seq := shm.DecodeHeader(headerBlk)

	stripe := &shm.Stripe{
		DataLen:    dataLen,
		Sequence:   seq,
		BlockCount: blkCnt,
		// Initialize the slice to hold the stripe blocks.
		// Note: Actual block data is not populated here as it is
		// not required for this expansion request.
		Blocks: make([][]byte, blkCnt),
	}
	stripe.Blocks[0] = headerBlk

	if h.ctrl.GetStripeOwner(slotID) != uint32(nodeID) {
		h.expandResponse(nodeID, slotID, shm.CMD_EXPAND_ERROR)
		slot := h.waiters[slotID].Load()
		if slot != nil {
			*slot <- fmt.Errorf("Hub: Node %d requested expand for Slot %d but is not the owner\n", nodeID, slotID)
		}
		return
	}

	err := h.bus.AllocBlocksForStripe(stripe, 2)
	if err != nil {
		h.expandResponse(nodeID, slotID, shm.CMD_EXPAND_ERROR)
		slot := h.waiters[slotID].Load()
		if slot != nil {
			*slot <- err
		}
		return
	}

	shm.UpdateStripeHeader(stripe)

	h.expandResponse(nodeID, slotID, shm.CMD_EXPAND_READY)
}

func (h *Hub) expandResponse(nodeID int, slotID uint32, cmd uint32) {
	reqQOff := shm.GetNodeReqQueueOffset(nodeID)
	h.ctrl.Push(reqQOff, slotID, cmd)

	h.mu.RLock()
	for _, n := range h.nodes {
		if n.nodeID == nodeID {
			n.nodeEv.Notify()
			break
		}
	}
	h.mu.RUnlock()
}
