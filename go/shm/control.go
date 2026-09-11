//go:build linux

package shm

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Control provides a view and access methods for the Bus Control Stripe.
type Control struct {
	cursor atomic.Uint32
	stripe *Stripe
}

// NewControl creates a Control accessor for a given Stripe.
func NewControl(stripe *Stripe) *Control {
	return &Control{stripe: stripe}
}

// --- Internal Atomic Accessors ---

func (c *Control) readU32(offset uintptr) uint32 {
	ptr := c.stripe.GetPointer(offset)
	if ptr == nil {
		return 0
	}
	return atomic.LoadUint32((*uint32)(ptr))
}

func (c *Control) writeU32(offset uintptr, val uint32) {
	ptr := c.stripe.GetPointer(offset)
	if ptr == nil {
		return
	}
	atomic.StoreUint32((*uint32)(ptr), val)
}

func (c *Control) casU32(offset uintptr, old, new uint32) bool {
	ptr := c.stripe.GetPointer(offset)
	if ptr == nil {
		return false
	}
	return atomic.CompareAndSwapUint32((*uint32)(ptr), old, new)
}

// --- Public Control Methods ---

func (c *Control) Init() {
	// Init RespQueue
	c.writeU32(OFF_RESP_QUEUE+Q_OFF_HEAD, 0)
	c.writeU32(OFF_RESP_QUEUE+Q_OFF_TAIL, 0)

	// Init All Node Request Queues
	for i := 0; i < MaxNodes; i++ {
		qOff := GetNodeReqQueueOffset(i)
		c.writeU32(qOff+Q_OFF_HEAD, 0)
		c.writeU32(qOff+Q_OFF_TAIL, 0)
	}

	// Init All Stripe Registry Entries to Idle
	for i := uint32(0); i < MaxTotalSlots; i++ {
		c.writeU32(GetStripeEntryOffset(i)+STRIPE_OFF_STATUS, StripeStatusIdle)
	}
}

// Push pushes a SlotID to a specified queue offset (MPSC Safe).
func (c *Control) Push(qOffset uintptr, slotID uint32, cmd uint32) bool {
	spin := 0

	for {
		head := c.readU32(qOffset + Q_OFF_HEAD)
		tail := c.readU32(qOffset + Q_OFF_TAIL)

		if (tail+1)%QueueSize == head {
			if spin < 10 {
				Procyield(30)
			} else if spin < 50 {
				runtime.Gosched()
			} else if spin < 75 {
				time.Sleep(time.Millisecond)
			} else {
				return false
			}
			spin++
			continue
		}

		if c.casU32(qOffset+Q_OFF_TAIL, tail, (tail+1)%QueueSize) {
			entryOff := qOffset + Q_OFF_ENTRIES + uintptr(tail*8)

			packed := (uint64(cmd|Q_READY_BIT) << 32) | uint64(slotID)
			ptr := c.stripe.GetPointer(entryOff)
			atomic.StoreUint64((*uint64)(ptr), packed)

			return true
		}

	}
}

// Pop pops a SlotID from a specified queue offset (Single Consumer Safe).
func (c *Control) Pop(qOffset uintptr) (uint32, uint32, bool) {
	head := c.readU32(qOffset + Q_OFF_HEAD)
	tail := c.readU32(qOffset + Q_OFF_TAIL)

	if head == tail {
		return 0, 0, false
	}

	entryOff := qOffset + Q_OFF_ENTRIES + uintptr(head*8)
	ptr := c.stripe.GetPointer(entryOff)

	for spin := 0; ; spin++ {
		packed := atomic.LoadUint64((*uint64)(ptr))
		cmd := uint32(packed >> 32)
		slotID := uint32(packed & 0xFFFFFFFF)

		if (cmd & Q_READY_BIT) != 0 {
			atomic.StoreUint64((*uint64)(ptr), 0)
			c.writeU32(qOffset+Q_OFF_HEAD, (head+1)%QueueSize)
			return slotID, cmd & (^uint32(Q_READY_BIT)), true
		}

		if spin < 10 {
			Procyield(30)
		} else if spin < 50 {
			runtime.Gosched()
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

// --- Stripe Registry Methods ---

func (c *Control) AllocSlot() (uint32, bool) {
	start := c.cursor.Add(1)
	for i := uint32(0); i < MaxTotalSlots; i++ {
		slot := (start+i-1)%MaxTotalSlots + 1
		off := GetStripeEntryOffset(slot) + STRIPE_OFF_STATUS
		if c.casU32(off, StripeStatusIdle, StripeStatusBusy) {
			return slot, true
		}
	}
	return 0, false
}

func (c *Control) FreeSlot(slotID uint32) {
	off := GetStripeEntryOffset(slotID) + STRIPE_OFF_STATUS
	c.writeU32(off, StripeStatusIdle)
}

func (c *Control) SetStripe(slotID, status uint32, headerCID, headerOff int16, owner uint32) {
	base := GetStripeEntryOffset(slotID)

	combinedHeader := uint32(uint16(headerCID))<<16 | uint32(uint16(headerOff))

	c.writeU32(base+STRIPE_OFF_HEADER, combinedHeader)
	c.writeU32(base+STRIPE_OFF_OWNER, owner)
	// Status should be set last
	c.writeU32(base+STRIPE_OFF_STATUS, status)
}

func (c *Control) GetStripeStatus(slotID uint32) uint32 {
	return c.readU32(GetStripeEntryOffset(slotID) + STRIPE_OFF_STATUS)
}

func (c *Control) GetStripeHeader(slotID uint32) (cid, off int16) {
	base := GetStripeEntryOffset(slotID)
	val := c.readU32(base + STRIPE_OFF_HEADER)
	cid = int16(uint16(val >> 16))
	off = int16(uint16(val & 0xFFFF))
	return
}

func (c *Control) GetStripeOwner(slotID uint32) (owner uint32) {
	base := GetStripeEntryOffset(slotID)
	owner = c.readU32(base + STRIPE_OFF_OWNER)
	return
}

func (c *Control) SetStripeStatus(slotID, status uint32) {
	base := GetStripeEntryOffset(slotID)
	c.writeU32(base+STRIPE_OFF_STATUS, status)
}
