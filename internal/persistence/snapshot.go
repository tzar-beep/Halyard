package persistence

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/tzar-beep/redis/internal/store"
)

// Snapshot file layout (all integers big-endian unless noted):
//
//	magic    "GRSNAP"            6 bytes
//	version  uint16              currently 1
//	gen      uint64              first AOF segment to replay after loading
//	count    uint64              number of records
//	records  count × {
//	           type     byte     0 = string
//	           expireAt int64    unix ms, 0 = no expiry
//	           keyLen   uvarint, key bytes
//	           valLen   uvarint, value bytes
//	         }
//	eof      byte 0xFF
//	crc      uint64              CRC-64/ECMA of every preceding byte
//
// Length-prefixed fields keep keys and values binary-safe.
//
// Version 2 adds container types; the value encoding depends on type:
//
//	0 string  valLen uvarint, bytes
//	1 list    count uvarint, count × (len, bytes)          head to tail
//	2 hash    count uvarint, count × (field, value)
//	3 set     count uvarint, count × member
//	4 zset    count uvarint, count × (member, score float64 bits)  ascending
//
// Version 1 files (strings only) are still readable.
const (
	snapMagic   = "GRSNAP"
	snapVersion = 2
	opEOF       = 0xFF
	maxFieldLen = 512 << 20
	maxElems    = 1 << 32
)

func readObject(kind store.Kind, readBytes func() ([]byte, error), readCount func() (int, error), br io.Reader) (*store.Object, error) {
	if kind == store.KindString {
		v, err := readBytes()
		return store.NewString(v), err
	}
	n, err := readCount()
	if err != nil {
		return nil, err
	}
	o := store.New(kind)
	for j := 0; j < n; j++ {
		m, err := readBytes()
		if err != nil {
			return nil, err
		}
		switch kind {
		case store.KindList:
			o.List.PushBack(m)
		case store.KindHash:
			v, err := readBytes()
			if err != nil {
				return nil, err
			}
			o.Hash[string(m)] = v
		case store.KindSet:
			o.Set[string(m)] = struct{}{}
		case store.KindZSet:
			var b [8]byte
			if _, err := io.ReadFull(br, b[:]); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
			}
			s := math.Float64frombits(binary.BigEndian.Uint64(b[:]))
			if math.IsNaN(s) {
				return nil, fmt.Errorf("%w: NaN score", ErrBadSnapshot)
			}
			o.ZSet.Add(string(m), s)
		}
	}
	if n == 0 {
		return nil, fmt.Errorf("%w: empty container", ErrBadSnapshot)
	}
	return o, nil
}

var crcTable = crc64.MakeTable(crc64.ECMA)

var ErrBadSnapshot = errors.New("snapshot corrupt")

// WriteSnapshot atomically writes entries to path: temp file, fsync, rename, fsync dir.
// A crash at any point leaves either the old file or the complete new one.
func WriteSnapshot(path string, gen uint64, entries []store.Entry) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // no-op after a successful rename
	if err := encodeSnapshot(f, gen, entries); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// EncodeSnapshot returns the snapshot encoding of entries in memory (used to ship
// state machine snapshots through Raft).
func EncodeSnapshot(gen uint64, entries []store.Entry) []byte {
	var b bytes.Buffer
	encodeSnapshot(&b, gen, entries)
	return b.Bytes()
}

// DecodeSnapshot is ReadSnapshot for an in-memory encoding.
func DecodeSnapshot(b []byte, restore func(store.Entry)) (uint64, error) {
	return decodeSnapshot(bytes.NewReader(b), int64(len(b)), restore)
}

func encodeSnapshot(out io.Writer, gen uint64, entries []store.Entry) error {
	crc := crc64.New(crcTable)
	bw := bufio.NewWriterSize(io.MultiWriter(out, crc), 1<<16)
	var buf [binary.MaxVarintLen64]byte
	putU64 := func(v uint64) { binary.BigEndian.PutUint64(buf[:8], v); bw.Write(buf[:8]) }
	putBytes := func(b []byte) { bw.Write(buf[:binary.PutUvarint(buf[:], uint64(len(b)))]); bw.Write(b) }

	bw.WriteString(snapMagic)
	binary.BigEndian.PutUint16(buf[:2], snapVersion)
	bw.Write(buf[:2])
	putU64(gen)
	putU64(uint64(len(entries)))
	putCount := func(n int) { bw.Write(buf[:binary.PutUvarint(buf[:], uint64(n))]) }
	for _, e := range entries {
		o := e.Obj
		bw.WriteByte(byte(o.Kind))
		putU64(uint64(e.ExpireAt))
		putBytes([]byte(e.Key))
		switch o.Kind {
		case store.KindString:
			putBytes(o.Str)
		case store.KindList:
			putCount(o.List.Len())
			for i := 0; i < o.List.Len(); i++ {
				putBytes(o.List.At(i))
			}
		case store.KindHash:
			putCount(len(o.Hash))
			for f, v := range o.Hash {
				putBytes([]byte(f))
				putBytes(v)
			}
		case store.KindSet:
			putCount(len(o.Set))
			for m := range o.Set {
				putBytes([]byte(m))
			}
		case store.KindZSet:
			putCount(o.ZSet.Len())
			o.ZSet.Each(func(m string, s float64) {
				putBytes([]byte(m))
				putU64(math.Float64bits(s))
			})
		}
	}
	bw.WriteByte(opEOF)
	if err := bw.Flush(); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(buf[:8], crc.Sum64())
	_, err := out.Write(buf[:8])
	return err
}

// ReadSnapshot verifies the checksum, then streams records to restore, skipping keys
// already expired. Returns the AOF generation recorded in the header.
func ReadSnapshot(path string, restore func(store.Entry)) (gen uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return decodeSnapshot(f, st.Size(), restore)
}

func decodeSnapshot(f io.ReadSeeker, size int64, restore func(store.Entry)) (gen uint64, err error) {
	// Pass 1: checksum, so corrupt length fields are never trusted for allocation.
	if size < int64(len(snapMagic))+2+8+8+1+8 {
		return 0, fmt.Errorf("%w: file too short", ErrBadSnapshot)
	}
	body := size - 8
	crc := crc64.New(crcTable)
	if _, err := io.Copy(crc, io.LimitReader(f, body)); err != nil {
		return 0, err
	}
	var sum [8]byte
	if _, err := io.ReadFull(f, sum[:]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint64(sum[:]) != crc.Sum64() {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrBadSnapshot)
	}

	// Pass 2: parse.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	br := bufio.NewReaderSize(io.LimitReader(f, body), 1<<16)
	head := make([]byte, len(snapMagic)+2+8+8)
	if _, err := io.ReadFull(br, head); err != nil {
		return 0, fmt.Errorf("%w: header: %v", ErrBadSnapshot, err)
	}
	if !bytes.Equal(head[:len(snapMagic)], []byte(snapMagic)) {
		return 0, fmt.Errorf("%w: bad magic", ErrBadSnapshot)
	}
	version := binary.BigEndian.Uint16(head[6:8])
	if version < 1 || version > snapVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrBadSnapshot, version)
	}
	gen = binary.BigEndian.Uint64(head[8:16])
	count := binary.BigEndian.Uint64(head[16:24])

	readBytes := func() ([]byte, error) {
		n, err := binary.ReadUvarint(br)
		if err != nil || n > maxFieldLen {
			return nil, fmt.Errorf("%w: bad length", ErrBadSnapshot)
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(br, b); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
		}
		return b, nil
	}
	readCount := func() (int, error) {
		n, err := binary.ReadUvarint(br)
		if err != nil || n > maxElems {
			return 0, fmt.Errorf("%w: bad element count", ErrBadSnapshot)
		}
		return int(n), nil
	}
	now := time.Now().UnixMilli()
	var fixed [9]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(br, fixed[:]); err != nil {
			return 0, fmt.Errorf("%w: record %d: %v", ErrBadSnapshot, i, err)
		}
		kind := store.Kind(fixed[0])
		if kind > store.KindZSet || (version == 1 && kind != store.KindString) {
			return 0, fmt.Errorf("%w: record %d: unknown type %d", ErrBadSnapshot, i, fixed[0])
		}
		exp := int64(binary.BigEndian.Uint64(fixed[1:]))
		key, err := readBytes()
		if err != nil {
			return 0, err
		}
		o, err := readObject(kind, readBytes, readCount, br)
		if err != nil {
			return 0, err
		}
		if exp != 0 && exp <= now {
			continue
		}
		restore(store.Entry{Key: string(key), Obj: o, ExpireAt: exp})
	}
	if b, err := br.ReadByte(); err != nil || b != opEOF {
		return 0, fmt.Errorf("%w: missing EOF marker", ErrBadSnapshot)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return 0, fmt.Errorf("%w: trailing data", ErrBadSnapshot)
	}
	return gen, nil
}
