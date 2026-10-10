package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes RESP2 replies into a buffered writer. Call Flush to send.
type Writer struct {
	bw  *bufio.Writer
	num []byte
}

func NewWriter(w io.Writer) *Writer { return &Writer{bw: bufio.NewWriter(w), num: make([]byte, 0, 20)} }

func (w *Writer) SimpleString(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) Error(msg string) {
	w.bw.WriteByte('-')
	w.bw.WriteString(msg)
	w.bw.WriteString("\r\n")
}

func (w *Writer) Integer(n int64) { w.prefixed(':', n) }

func (w *Writer) Bulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) NullBulk() { w.bw.WriteString("$-1\r\n") }

func (w *Writer) ArrayHeader(n int) { w.prefixed('*', int64(n)) }

func (w *Writer) Flush() error { return w.bw.Flush() }

func (w *Writer) prefixed(p byte, n int64) {
	w.bw.WriteByte(p)
	w.num = strconv.AppendInt(w.num[:0], n, 10)
	w.bw.Write(w.num)
	w.bw.WriteString("\r\n")
}
