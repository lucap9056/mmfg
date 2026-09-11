//go:build linux

package netutil

import (
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
