//go:build unix

package netutil

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// MsgType defines the protocol message types over UDS.
type MsgType uint8

const (
	MsgHandshake MsgType = 1
	MsgNewChunk  MsgType = 2
	MsgHeartbeat MsgType = 3
	MsgRelease   MsgType = 4
)

// SendFDs sends multiple file descriptors over a Unix Domain Socket.
func SendFDs(conn net.Conn, fds ...int) error {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("not a unix connection")
	}

	// We send a dummy byte with the OOB data
	oob := unix.UnixRights(fds...)
	_, _, err := uc.WriteMsgUnix([]byte{0}, oob, nil)
	return err
}

// ReceiveFDs receives multiple file descriptors over a Unix Domain Socket.
func ReceiveFDs(conn net.Conn, count int) ([]int, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("not a unix connection")
	}

	buf := make([]byte, 1)
	oob := make([]byte, unix.CmsgSpace(count*4))
	n, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, fmt.Errorf("failed to read data byte")
	}

	scm, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, err
	}
	return unix.ParseUnixRights(&scm[0])
}

func SendMsgWithFDs(conn *net.UnixConn, msg []byte, fds ...int) error {
	oob := unix.UnixRights(fds...)
	_, _, err := conn.WriteMsgUnix(msg, oob, nil)
	return err
}

func ReceiveMsgWithFDs(conn *net.UnixConn, msg []byte, oob []byte) (n int, fds []int, err error) {
	n, oobn, _, _, err := conn.ReadMsgUnix(msg, oob)
	if err != nil {
		return n, nil, err
	}

	defer func() {
		if err != nil {
			for _, fd := range fds {
				unix.Close(fd)
			}
			fds = nil
		}
	}()

	scms, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return n, nil, err
	}

	for _, scm := range scms {
		if scmFDs, err := unix.ParseUnixRights(&scm); err != nil {
			return n, nil, err
		} else {
			fds = append(fds, scmFDs...)
		}
	}

	return n, fds, nil
}
