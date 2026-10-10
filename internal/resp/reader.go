// Package resp implements the RESP2 wire protocol.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	MaxArgs     = 1 << 20  // max elements in a request array
	MaxBulkLen  = 64 << 20 // max bytes in one bulk string
	MaxInline   = 64 << 10 // max inline request line length
	preallocCap = 64       // never trust declared array length for allocation

	arenaSize  = 16 << 10
	largeValue = 64 << 10 // bigger bulk strings get their own allocation
)

// ProtocolError means the client sent malformed input; the connection should be closed.
type ProtocolError struct{ msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.msg }

func protoErr(format string, a ...any) error { return &ProtocolError{fmt.Sprintf(format, a...)} }

// Reader parses requests. To avoid per-argument allocations, the arguments returned
// by ReadCommand point into buffers reused by the next call: callers must copy
// anything they keep (the store clones values; keys become strings).
type Reader struct {
	br    *bufio.Reader
	arena []byte   // backing storage for the current command's arguments
	args  [][]byte // reused argument slice
}

func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReader(r)} }

// Buffered reports bytes already read from the connection but not yet parsed.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one request: a RESP array of bulk strings, or an inline command.
// Returns an empty slice for empty requests.
func (r *Reader) ReadCommand() ([][]byte, error) {
	b, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] != '*' {
		return r.readInline()
	}
	r.br.ReadByte()
	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if n <= 0 { // *0 and *-1 are no-ops
		return [][]byte{}, nil
	}
	if n > MaxArgs {
		return nil, protoErr("invalid multibulk length")
	}
	r.arena = r.arena[:0]
	if cap(r.args) > preallocCap*16 { // don't pin a huge slice after one big request
		r.args = nil
	}
	args := r.args[:0]
	if args == nil {
		args = make([][]byte, 0, min(n, preallocCap))
	}
	for i := 0; i < n; i++ {
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	r.args = args
	return args, nil
}

// alloc returns n bytes for an argument, from the arena when it fits. When the
// arena is full a new one is started; earlier arguments keep the old one alive.
func (r *Reader) alloc(n int) []byte {
	if n > largeValue {
		return make([]byte, n)
	}
	if len(r.arena)+n > cap(r.arena) {
		r.arena = make([]byte, 0, max(arenaSize, 2*n))
	}
	start := len(r.arena)
	r.arena = r.arena[:start+n]
	return r.arena[start : start+n : start+n]
}

func (r *Reader) readBulk() ([]byte, error) {
	c, err := r.br.ReadByte()
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if c != '$' {
		return nil, protoErr("expected '$', got '%c'", c)
	}
	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if n < 0 || n > MaxBulkLen {
		return nil, protoErr("invalid bulk length")
	}
	buf := r.alloc(n)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, unexpectedEOF(err)
	}
	var crlf [2]byte
	if _, err := io.ReadFull(r.br, crlf[:]); err != nil {
		return nil, unexpectedEOF(err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf, nil
}

// readInt reads a decimal integer terminated by CRLF.
func (r *Reader) readInt() (int, error) {
	line, err := r.readLine(32)
	if err != nil {
		return 0, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return 0, protoErr("expected CRLF")
	}
	n, ok := parseInt(line[:len(line)-2])
	if !ok {
		return 0, protoErr("invalid length")
	}
	return n, nil
}

// parseInt parses a decimal length without allocating.
func parseInt(b []byte) (int, bool) {
	neg := len(b) > 0 && b[0] == '-'
	if neg {
		b = b[1:]
	}
	if len(b) == 0 || len(b) > 18 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

// readLine reads through '\n' (inclusive), rejecting lines longer than max bytes.
// The common case returns a slice of the read buffer (valid until the next read).
func (r *Reader) readLine(max int) ([]byte, error) {
	frag, err := r.br.ReadSlice('\n')
	if err == nil {
		if len(frag) > max {
			return nil, protoErr("line too long")
		}
		return frag, nil
	}
	line := append([]byte(nil), frag...)
	for {
		if len(line) > max {
			return nil, protoErr("line too long")
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, unexpectedEOF(err)
		}
		frag, err = r.br.ReadSlice('\n')
		line = append(line, frag...)
		if err == nil {
			if len(line) > max {
				return nil, protoErr("line too long")
			}
			return line, nil
		}
	}
}

func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine(MaxInline)
	if err != nil {
		return nil, err
	}
	return bytes.Fields(append([]byte(nil), line...)), nil
}

// unexpectedEOF converts EOF mid-request into ErrUnexpectedEOF.
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
