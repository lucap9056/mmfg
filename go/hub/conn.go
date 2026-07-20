//go:build unix

package hub

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

var donePool = sync.Pool{
	New: func() interface{} {
		return make(chan error, 1)
	},
}

func (h *Hub) Request(ctx context.Context, blockCount int, needSlot bool) (Connection, error) {
	stripe, err := h.bus.AllocStripe(blockCount + 1)
	if err != nil {
		return nil, err
	}

	conn := &busConn{hub: h, Stripe: stripe}

	if needSlot {
		slotID, err := h.allocSlot(stripe)
		if err != nil {
			h.bus.FreeStripe(stripe)
			return nil, fmt.Errorf("no slots available")
		}

		conn.done = donePool.Get().(chan error)
		conn.slotID = slotID
	}

	return conn, nil
}

type Connection interface {
	io.Writer
	io.Reader
	io.WriterAt
	io.ReaderAt
	io.ByteReader
	io.ByteWriter
	DataLen() uint32
	Next(nodeName string) error
	WriteTo(w io.Writer) (int64, error)
	Close()
	View(offset, length int, call func(*shm.Viewer) error) error
}

type busConn struct {
	hub    *Hub
	node   *NodeInfo
	Stripe *shm.Stripe

	readOff  int64
	writeOff int64
	slotID   uint32

	done chan error
	mux  sync.Mutex
}

func (c *busConn) DataLen() uint32 {
	return c.Stripe.DataLen
}

func (c *busConn) View(offset, length int, call func(*shm.Viewer) error) error {
	return c.Stripe.View(offset, length, call)
}

func (c *busConn) ensureCapacity(needed int) error {
	userCap := (c.Stripe.BlockCount - 1) * shm.BlockSize
	if needed <= userCap {
		return nil
	}

	extraBytes := needed - userCap
	blocksNeeded := (extraBytes + shm.BlockSize - 1) / shm.BlockSize

	err := c.hub.bus.AllocBlocksForStripe(c.Stripe, blocksNeeded)
	if err != nil {
		return err
	}

	return nil
}

func (c *busConn) WriteByte(b byte) error {
	if err := c.ensureCapacity(int(c.writeOff) + 1); err != nil {
		return err
	}
	err := c.Stripe.WriteByteAt(b, c.writeOff)
	if err == nil {
		c.writeOff++
	}
	return err
}

func (c *busConn) Write(data []byte) (int, error) {
	n, err := c.WriteAt(data, c.writeOff)
	if err != nil {
		return n, err
	}
	c.writeOff += int64(n)
	return n, nil
}

func (c *busConn) WriteAt(data []byte, off int64) (int, error) {
	if err := c.ensureCapacity(int(off) + len(data)); err != nil {
		return 0, err
	}

	n, err := c.Stripe.WriteAt(data, off)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (c *busConn) ReadByte() (byte, error) {
	b, err := c.Stripe.ReadByteAt(c.readOff)
	if err == nil {
		c.readOff++
	}
	return b, err
}

func (c *busConn) Read(data []byte) (int, error) {
	n, err := c.ReadAt(data, c.readOff)
	if err != nil {
		return n, err
	}
	c.readOff += int64(n)
	return n, nil
}

func (c *busConn) ReadAt(data []byte, off int64) (int, error) {
	return c.Stripe.ReadAt(data, off)
}

func (c *busConn) Next(nodeName string) error {

	c.hub.mu.RLock()
	targetNode, ok := c.hub.nodes[nodeName]
	c.hub.mu.RUnlock()
	if !ok {
		return fmt.Errorf("target node not found")
	}
	c.node = targetNode

	if c.slotID == 0 {
		slotID, err := c.hub.allocSlot(c.Stripe)
		if err != nil {
			return err
		}
		c.done = donePool.Get().(chan error)
		c.slotID = slotID
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status := c.hub.ctrl.GetStripeStatus(c.slotID)
		if status == shm.StripeStatusDone || status == shm.StripeStatusReady {
			break
		}
		time.Sleep(10 * time.Microsecond)
	}

	cid, off := c.Stripe.Sequence[0], c.Stripe.Sequence[1]
	c.hub.ctrl.SetStripe(c.slotID, shm.StripeStatusBusy, cid, off, uint32(targetNode.nodeID))

	clearChannel(c.done)
	c.hub.waiters[c.slotID].Store(&c.done)
	if err := c.hub.submit(targetNode, c.slotID); err != nil {
		return err
	}
	c.writeOff = 0
	c.readOff = 0
	err := <-c.done
	if err != nil {
		return err
	}

	return c.refreshStripe()
}

func clearChannel(ch chan error) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (c *busConn) WriteTo(w io.Writer) (int64, error) {
	dataLen := c.Stripe.DataLen
	return io.Copy(w, io.LimitReader(c, int64(dataLen)))
}

func (c *busConn) refreshStripe() error {
	if c.Stripe == nil {
		return fmt.Errorf("no active request")
	}

	dataLen, blkCnt, seq := shm.DecodeHeader(c.Stripe.Blocks[0])

	if blkCnt != c.Stripe.BlockCount {
		c.Stripe.BlockCount = blkCnt

		l := len(c.Stripe.Sequence)

		var lastChunkID int16
		for i, id := range seq {
			if id < 0 {
				lastChunkID = id
				continue
			}
			if i < l {
				continue
			}

			chunk := c.hub.bus.GetChunk(lastChunkID)
			off := int(id) * shm.BlockSize
			block := chunk.Data[off : off+shm.BlockSize]
			c.Stripe.Blocks = append(c.Stripe.Blocks, block)
		}
		c.Stripe.Sequence = seq

	}

	c.Stripe.DataLen = dataLen
	return nil
}

func (c *busConn) Close() {
	c.hub.bus.FreeStripe(c.Stripe)
	c.Stripe = nil
	if c.slotID != 0 {
		c.hub.waiters[c.slotID].Store(nil)
		donePool.Put(c.done)
		c.hub.ctrl.FreeSlot(c.slotID)
		c.done = nil
		select {
		case <-c.hub.limit:
		default:
		}
	}
}
