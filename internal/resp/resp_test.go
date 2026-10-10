package resp

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func parseAll(t *testing.T, r io.Reader) [][]string {
	t.Helper()
	rd := NewReader(r)
	var out [][]string
	for {
		args, err := rd.ReadCommand()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var s []string
		for _, a := range args {
			s = append(s, string(a))
		}
		out = append(out, s)
	}
}

func TestReadArray(t *testing.T) {
	got := parseAll(t, bytes.NewBufferString("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	if len(got) != 1 || len(got[0]) != 3 || got[0][2] != "bar" {
		t.Fatalf("got %q", got)
	}
}

func TestPipelinedAndPartialReads(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhi\r\nx\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	// OneByteReader forces every read to return a single byte.
	got := parseAll(t, iotest.OneByteReader(bytes.NewBufferString(in)))
	if len(got) != 3 || got[1][1] != "hi\r\nx" || got[2][1] != "k" {
		t.Fatalf("got %q", got)
	}
}

func TestBinarySafe(t *testing.T) {
	got := parseAll(t, bytes.NewBufferString("*1\r\n$3\r\na\x00b\r\n"))
	if got[0][0] != "a\x00b" {
		t.Fatalf("got %q", got)
	}
}

func TestInline(t *testing.T) {
	got := parseAll(t, bytes.NewBufferString("PING\r\nset a  b\n"))
	if len(got) != 2 || got[0][0] != "PING" || len(got[1]) != 3 {
		t.Fatalf("got %q", got)
	}
}

func TestMalformed(t *testing.T) {
	cases := map[string]string{
		"bad array len":   "*x\r\n",
		"bad bulk prefix": "*1\r\n:3\r\nfoo\r\n",
		"neg bulk len":    "*1\r\n$-5\r\n",
		"huge bulk len":   "*1\r\n$999999999999\r\n",
		"huge array":      "*99999999\r\n",
		"missing CRLF":    "*1\r\n$3\r\nfooXY",
		"LF only":         "*1\n",
	}
	for name, in := range cases {
		_, err := NewReader(bytes.NewBufferString(in)).ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Errorf("%s: want ProtocolError, got %v", name, err)
		}
	}
}

func TestTruncated(t *testing.T) {
	_, err := NewReader(bytes.NewBufferString("*2\r\n$3\r\nfoo\r\n$3\r\nba")).ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestWriter(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	w.SimpleString("OK")
	w.Error("ERR x")
	w.Integer(-42)
	w.Bulk([]byte("foo"))
	w.NullBulk()
	w.ArrayHeader(2)
	w.Flush()
	want := "+OK\r\n-ERR x\r\n:-42\r\n$3\r\nfoo\r\n$-1\r\n*2\r\n"
	if b.String() != want {
		t.Fatalf("got %q want %q", b.String(), want)
	}
}

func FuzzReadCommand(f *testing.F) {
	for _, s := range []string{"*1\r\n$4\r\nPING\r\n", "*0\r\n", "*1\r\n$-1\r\n", "*1\r\n$3\r\nfo", "PING\r\n", "*2\r\n$1\r\na\r\n$1\r\n\x00\r\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		rd := NewReader(bytes.NewReader(in))
		for i := 0; i < 100; i++ {
			if _, err := rd.ReadCommand(); err != nil {
				return
			}
		}
	})
}
