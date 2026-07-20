package sync

import (
	"bytes"
	"net"

	"github.com/nautrouds/mmfg/internal/netutil"
	"golang.org/x/sys/unix"
)

var (
	CONN_HEADER  = []byte("MMFG")
	CONN_VERSION = byte(2)
)

func HandshakeMessage(nodeID int) []byte {
	msg := make([]byte, 0, 7)
	msg = append(msg, CONN_HEADER...)
	msg = append(
		msg, CONN_VERSION,
		byte(netutil.MsgHandshake),
		byte(nodeID),
	)
	return msg
}

func NewChunkMessage() []byte {
	return []byte{
		byte(netutil.MsgNewChunk),
	}
}

func HeartbeatMessage() []byte {
	return []byte{
		byte(netutil.MsgHeartbeat),
	}
}

func ReleaseMessage() []byte {
	return []byte{
		byte(netutil.MsgRelease),
	}
}

func IsMMFG(conn *net.UnixConn) (bool, error) {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}

	buf := make([]byte, len(CONN_HEADER))
	var n int
	var peekErr error

	err = rawConn.Read(func(fd uintptr) bool {
		n, _, peekErr = unix.Recvfrom(int(fd), buf, unix.MSG_PEEK)
		if peekErr == unix.EAGAIN {
			return false
		}
		return true
	})
	if err != nil {
		return false, err
	}
	if peekErr != nil {
		return false, peekErr
	}
	if n < len(CONN_HEADER) {
		return false, nil
	}

	return bytes.Equal(buf[:n], CONN_HEADER), nil
}
