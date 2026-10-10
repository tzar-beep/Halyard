package persistence

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
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
const (
	snapMagic   = "GRSNAP"
	snapVersion = 1
	typeString  = 0
	opEOF       = 0xFF
	maxFieldLen = 512 << 20
)

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

	crc := crc64.New(crcTable)
	bw := bufio.NewWriterSize(io.MultiWriter(f, crc), 1<<16)
	var buf [binary.MaxVarintLen64]byte
	putU64 := func(v uint64) { binary.BigEndian.PutUint64(buf[:8], v); bw.Write(buf[:8]) }
	putBytes := func(b []byte) { bw.Write(buf[:binary.PutUvarint(buf[:], uint64(len(b)))]); bw.Write(b) }

	bw.WriteString(snapMagic)
	binary.BigEndian.PutUint16(buf[:2], snapVersion)
	bw.Write(buf[:2])
	putU64(gen)
	putU64(uint64(len(entries)))
	for _, e := range entries {
		bw.WriteByte(typeString)
		putU64(uint64(e.ExpireAt))
		putBytes([]byte(e.Key))
		putBytes(e.Val)
	}
	bw.WriteByte(opEOF)
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	binary.BigEndian.PutUint64(buf[:8], crc.Sum64())
	if _, err := f.Write(buf[:8]); err != nil {
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

// ReadSnapshot verifies the checksum, then streams records to restore, skipping keys
// already expired. Returns the AOF generation recorded in the header.
func ReadSnapshot(path string, restore func(store.Entry)) (gen uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Pass 1: checksum, so corrupt length fields are never trusted for allocation.
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if st.Size() < int64(len(snapMagic))+2+8+8+1+8 {
		return 0, fmt.Errorf("%w: file too short", ErrBadSnapshot)
	}
	body := st.Size() - 8
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
	if v := binary.BigEndian.Uint16(head[6:8]); v != snapVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrBadSnapshot, v)
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
	now := time.Now().UnixMilli()
	var fixed [9]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(br, fixed[:]); err != nil {
			return 0, fmt.Errorf("%w: record %d: %v", ErrBadSnapshot, i, err)
		}
		if fixed[0] != typeString {
			return 0, fmt.Errorf("%w: record %d: unknown type %d", ErrBadSnapshot, i, fixed[0])
		}
		exp := int64(binary.BigEndian.Uint64(fixed[1:]))
		key, err := readBytes()
		if err != nil {
			return 0, err
		}
		val, err := readBytes()
		if err != nil {
			return 0, err
		}
		if exp != 0 && exp <= now {
			continue
		}
		restore(store.Entry{Key: string(key), Val: val, ExpireAt: exp})
	}
	if b, err := br.ReadByte(); err != nil || b != opEOF {
		return 0, fmt.Errorf("%w: missing EOF marker", ErrBadSnapshot)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return 0, fmt.Errorf("%w: trailing data", ErrBadSnapshot)
	}
	return gen, nil
}
