//go:build unix

package node

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nautrouds/mmfg/go/shm"
	mmfg_sync "github.com/nautrouds/mmfg/go/sync"
	"github.com/nautrouds/mmfg/internal/netutil"
	"golang.org/x/sys/unix"
)

type Connection interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	io.ByteReader
	io.ByteWriter
	DataLen() uint32
	RequestExpand() error
	OnExpandComplete()
	View(offset, length int, call func(*shm.Viewer) error) error
}

type Handler func(conn Connection)

type Node struct {
	nodeID  int
	ctrl    *shm.Control
	nodeEv  *mmfg_sync.Eventfd
	hubEv   *mmfg_sync.Eventfd
	chunks  []*shm.Chunk
	handler Handler
	mu      sync.RWMutex
	waiters map[uint32]chan bool
	wMu     sync.Mutex
}

type NodeOption func(*Node)

func WithHandler(handler Handler) NodeOption {
	return func(n *Node) {
		n.handler = handler
	}
}

func NewNode(opts ...NodeOption) *Node {
	n := &Node{
		chunks:  make([]*shm.Chunk, 0),
		waiters: make(map[uint32]chan bool),
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Listen binds socketPath and serves it. Convenience wrapper around Serve.
func (n *Node) Listen(socketPath string) error {
	os.Remove(socketPath)
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		return err
	}
	return n.Serve(l)
}

// Serve accepts from l and hands each conn to HandleConn. For a listener
// shared with other protocols, demux externally and call HandleConn directly.
func (n *Node) Serve(l *net.UnixListener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		uc, ok := conn.(*net.UnixConn)
		if !ok {
			continue
		}
		go n.HandleConn(uc)
	}
}

var maxOobLen = unix.CmsgSpace((shm.MaxChunks + 2) * 4)

// HandleConn runs the Hub handshake and event loop over an accepted conn.
// Callers are responsible for routing only mmfg traffic here.
func (n *Node) HandleConn(conn *net.UnixConn) {
	defer conn.Close()
	oob := make([]byte, maxOobLen)

	session, err := handshake(conn, oob)
	if err != nil {
		log.Printf("handshake failed: %v", err)
		return
	}

	n.loadSession(session)

	defer func() {
		n.mu.Lock()
		defer n.mu.Unlock()

		for _, c := range n.chunks {
			c.Close()
		}
		n.chunks = nil
		if n.nodeEv != nil {
			n.nodeEv.Close()
		}
		if n.hubEv != nil {
			n.hubEv.Close()
		}
	}()

	go n.StartEventLoop()

	for {
		if err := n.handleMessage(conn, oob); err != nil {
			log.Printf("")
			break
		}
	}
}

func (n *Node) handleMessage(conn *net.UnixConn, oob []byte) (err error) {
	msgHeader := make([]byte, 1)
	var fds []int
	_, fds, err = netutil.ReceiveMsgWithFDs(conn, msgHeader, oob)
	if err != nil {
		return
	}
	defer func() {
		for _, fd := range fds {
			unix.Close(fd)
		}
	}()

	switch netutil.MsgType(msgHeader[0]) {
	case netutil.MsgNewChunk:
		chunks := make([]*shm.Chunk, len(fds))

		defer func() {
			if err != nil {
				for _, c := range chunks {
					if c != nil {
						c.Close()
					}
				}
			}
		}()

		for i, fd := range fds {
			chunks[i], err = shm.AttachChunk(fd)
			if err != nil {
				return
			}
			chunks[i].Fd = -1
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		n.chunks = append(n.chunks, chunks...)

	case netutil.MsgRelease:
		// ACK or Stop

	default:
	}
	return
}

type nodeSession struct {
	NodeID        int
	NodeEv        int
	HubEv         int
	ControlStripe *shm.Stripe
	Chunks        []*shm.Chunk
}

func handshake(conn *net.UnixConn, oob []byte) (session *nodeSession, err error) {
	header := make([]byte, 7)
	n, fds, err := netutil.ReceiveMsgWithFDs(conn, header, oob)
	if err != nil {
		return
	}

	defer func() {
		for i, fd := range fds {
			if err != nil || i >= 2 {
				unix.Close(fd)
			}
		}
	}()

	if n != 7 {
		err = fmt.Errorf("short header: got %d bytes, want 7", n)
		return
	}

	if !bytes.Equal(header[:4], mmfg_sync.CONN_HEADER) {
		err = fmt.Errorf("Invalid handshake header: %s", string(header[:4]))
		return
	}

	if header[4] != mmfg_sync.CONN_VERSION {
		err = fmt.Errorf("Unsupported version: %d", header[4])
		return
	}

	nodeID := int(header[6])

	if nodeID <= 0 || nodeID >= shm.MaxNodes {
		err = fmt.Errorf("invalid nodeID %d", nodeID)
		return
	}

	if len(fds) < 3 {
		err = fmt.Errorf("expected at least 3 fds, got %d", len(fds))
		return
	}

	chunks := make([]*shm.Chunk, 0, len(fds)-2)
	defer func() {
		if err != nil {
			for _, c := range chunks {
				c.Close()
			}
		}
	}()

	blkCnt := shm.ControlStripeSize / shm.BlockSize
	controlStripe := &shm.Stripe{
		BlockCount: blkCnt,
		Blocks:     make([][]byte, blkCnt),
	}

	var nodeEv int
	var hubEv int

	for i, fd := range fds {

		switch i {
		case 0:
			nodeEv = fd
		case 1:
			hubEv = fd
		default:
			var chunk *shm.Chunk
			chunk, err = shm.AttachChunk(fd)
			if err != nil {
				return
			}

			if i == 2 {
				for j := range blkCnt {
					offset := j * shm.BlockSize
					controlStripe.Blocks[j] = chunk.Data[offset : offset+shm.BlockSize]
				}
			}

			chunks = append(chunks, chunk)
			chunk.Fd = -1
		}
	}

	session = &nodeSession{
		NodeID:        nodeID,
		NodeEv:        nodeEv,
		HubEv:         hubEv,
		ControlStripe: controlStripe,
		Chunks:        chunks,
	}
	return
}

func (n *Node) loadSession(s *nodeSession) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nodeID = s.NodeID
	n.ctrl = shm.NewControl(s.ControlStripe)
	n.nodeEv = mmfg_sync.AttachEventfd(s.NodeEv)
	n.hubEv = mmfg_sync.AttachEventfd(s.HubEv)
	n.chunks = s.Chunks
}

func (n *Node) processSlot(slotID uint32) {
	cid, off := n.ctrl.GetStripeHeader(slotID)

	headerChunk := n.getChunk(cid)
	offset := int(off) * shm.BlockSize
	headerBlk := headerChunk.Data[offset : offset+shm.BlockSize]

	dataLen, blkCnt, seq := shm.DecodeHeader(headerBlk)
	stripe := &shm.Stripe{
		DataLen:    dataLen,
		Sequence:   seq,
		BlockCount: blkCnt,
		Blocks:     make([][]byte, 0, blkCnt),
	}

	chunkID := int16(cid)
	for _, id := range stripe.Sequence {
		if id < 0 {
			chunkID = id
			continue
		}
		chunk := n.getChunk(chunkID)
		off := int(id) * shm.BlockSize
		stripe.Blocks = append(stripe.Blocks, chunk.Data[off:off+shm.BlockSize])
	}

	conn := &nodeConn{
		node:   n,
		slotID: slotID,
		stripe: stripe,
	}

	if n.handler != nil {
		n.handler(conn)
	}

	if conn.failed {
		return
	}

	n.ctrl.SetStripeStatus(slotID, shm.StripeStatusDone)

	respQOff := shm.GetNodeReqQueueOffset(0)
	if n.ctrl.Push(respQOff, slotID, shm.CMD_PROCESS) {
		n.hubEv.Notify()
	} else {
		fmt.Printf("Node %d: FAILED to push Slot %d to RespQueue\n", n.nodeID, slotID)
	}
}

type nodeConn struct {
	node   *Node
	stripe *shm.Stripe

	readOff  int64
	writeOff int64

	slotID uint32

	failed bool
}

func (c *nodeConn) DataLen() uint32 {
	return c.stripe.DataLen
}

func (c *nodeConn) RequestExpand() error {
	ch := make(chan bool)
	c.node.wMu.Lock()
	c.node.waiters[c.slotID] = ch
	c.node.wMu.Unlock()

	// Hub's Command queue (using 0 for hub)
	respQOff := uintptr(shm.OFF_RESP_QUEUE)
	if !c.node.ctrl.Push(respQOff, c.slotID, shm.CMD_REQUEST_EXPAND) {
		return fmt.Errorf("failed to push expand request")
	}
	c.node.hubEv.Notify()
	ok := <-ch
	close(ch)
	if !ok {
		c.failed = true
		return fmt.Errorf("failed to expand stripe")
	}
	return nil
}

func (c *nodeConn) OnExpandComplete() {
	cid, off := c.node.ctrl.GetStripeHeader(c.slotID)
	headerChunk := c.node.getChunk(cid)
	offset := int(off) * shm.BlockSize
	headerData := headerChunk.Data[offset : offset+shm.BlockSize]
	_, _, newSequence := shm.DecodeHeader(headerData)

	l := len(c.stripe.Sequence)

	var lastChunkID int16
	for i, id := range newSequence {
		if id < 0 {
			lastChunkID = id
			continue
		}
		if i < l {
			continue
		}
		chunk := c.node.getChunk(lastChunkID)
		off := int(id) * shm.BlockSize
		block := chunk.Data[off : off+shm.BlockSize]
		c.stripe.Blocks = append(c.stripe.Blocks, block)
	}
	c.stripe.Sequence = newSequence
}

func (n *Node) getChunk(cid int16) *shm.Chunk {
	if cid >= 0 {
		log.Printf("CID: %d\n", cid)
	}
	realCID := int(^cid)
	for {
		n.mu.RLock()
		if realCID < len(n.chunks) {
			chunk := n.chunks[realCID]
			n.mu.RUnlock()
			return chunk
		}
		n.mu.RUnlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *nodeConn) View(offset, length int, call func(*shm.Viewer) error) error {
	return c.stripe.View(offset, length, call)
}

func (c *nodeConn) ReadByte() (byte, error) {
	b, err := c.stripe.ReadByteAt(c.readOff)
	if err == nil {
		c.readOff++
	}
	return b, err
}

func (c *nodeConn) Read(p []byte) (int, error) {
	n, err := c.stripe.ReadAt(p, c.readOff)
	if err != nil {
		return n, err
	}

	c.readOff += int64(n)
	return n, err
}

func (c *nodeConn) ReadAt(p []byte, off int64) (int, error) {
	return c.stripe.ReadAt(p, off)
}

func (c *nodeConn) WriteByte(b byte) error {
	totalCap := c.stripe.BlockCount * shm.BlockSize
	needCap := int(c.writeOff) + 1
	userCap := totalCap - shm.BlockSize
	if needCap > shm.StripeDataSizeLimit {
		return fmt.Errorf("stripe block count limit reached, cannot expand further")
	}
	for needCap > userCap {
		if err := c.RequestExpand(); err != nil {
			return err
		}
		c.OnExpandComplete()
		totalCap = c.stripe.BlockCount * shm.BlockSize
		userCap = totalCap - shm.BlockSize
	}

	return c.stripe.WriteByteAt(b, c.writeOff)
}

func (c *nodeConn) Write(p []byte) (int, error) {
	n, err := c.WriteAt(p, c.writeOff)
	if err != nil {
		return 0, err
	}
	c.writeOff += int64(n)
	return n, err
}

func (c *nodeConn) WriteAt(p []byte, off int64) (int, error) {
	totalCap := c.stripe.BlockCount * shm.BlockSize
	needCap := int(off) + len(p)
	userCap := totalCap - shm.BlockSize
	if needCap > shm.StripeDataSizeLimit {
		return 0, fmt.Errorf("stripe block count limit reached, cannot expand further")
	}
	for needCap > userCap {
		if err := c.RequestExpand(); err != nil {
			return 0, err
		}
		c.OnExpandComplete()
		totalCap = c.stripe.BlockCount * shm.BlockSize
		userCap = totalCap - shm.BlockSize
	}

	return c.stripe.WriteAt(p, off)
}

func (n *Node) StartEventLoop() {
	reqQOff := shm.GetNodeReqQueueOffset(n.nodeID)

	for {
		err := n.nodeEv.Wait()
		if err != nil {
			fmt.Printf("Node %d: Event Loop stopping: %v\n", n.nodeID, err)
			return
		}
		for {
			slotID, cmd, ok := n.ctrl.Pop(reqQOff)
			if !ok {
				break
			}
			switch cmd {
			case shm.CMD_PROCESS:
				go n.processSlot(slotID)
			case shm.CMD_RELEASE:
				n.ctrl.FreeSlot(slotID)
			case shm.CMD_EXPAND_READY:
				n.wMu.Lock()
				if ch, ok := n.waiters[slotID]; ok {
					ch <- true
					delete(n.waiters, slotID)
				}
				n.wMu.Unlock()
			case shm.CMD_EXPAND_ERROR:
				n.wMu.Lock()
				if ch, ok := n.waiters[slotID]; ok {
					ch <- false
					delete(n.waiters, slotID)
				}
				n.wMu.Unlock()
			}
		}
	}
}
