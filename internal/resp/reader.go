// Package resp implements the RESP2 wire protocol.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	MaxArgs     = 1 << 20  // max elements in a request array
	MaxBulkLen  = 64 << 20 // max bytes in one bulk string
	MaxInline   = 64 << 10 // max inline request line length
	preallocCap = 64       // never trust declared array length for allocation
)

// ProtocolError means the client sent malformed input; the connection should be closed.
type ProtocolError struct{ msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.msg }

func protoErr(format string, a ...any) error { return &ProtocolError{fmt.Sprintf(format, a...)} }

type Reader struct{ br *bufio.Reader }

func NewReader(r io.Reader) *Reader { return &Reader{bufio.NewReader(r)} }

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
	args := make([][]byte, 0, min(n, preallocCap))
	for i := 0; i < n; i++ {
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
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
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, unexpectedEOF(err)
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:n:n], nil
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
	n, err := strconv.Atoi(string(line[:len(line)-2]))
	if err != nil {
		return 0, protoErr("invalid length")
	}
	return n, nil
}

// readLine reads through '\n' (inclusive), rejecting lines longer than max bytes.
func (r *Reader) readLine(max int) ([]byte, error) {
	var line []byte
	for {
		frag, err := r.br.ReadSlice('\n')
		line = append(line, frag...)
		if len(line) > max {
			return nil, protoErr("line too long")
		}
		if err == nil {
			return line, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, unexpectedEOF(err)
		}
	}
}

func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine(MaxInline)
	if err != nil {
		return nil, err
	}
	return bytes.Fields(line), nil
}

// unexpectedEOF converts EOF mid-request into ErrUnexpectedEOF.
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
