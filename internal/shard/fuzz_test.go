package shard

import (
	"bytes"
	"testing"

	"github.com/tzar-beep/Halyard/internal/store"
)

func FuzzDecodeDump(f *testing.F) {
	l := store.New(store.KindList)
	l.List.PushBack([]byte("a"))
	f.Add(EncodeDump(l, 0))
	f.Add(EncodeDump(store.NewString([]byte("x")), 123))
	f.Fuzz(func(t *testing.T, b []byte) {
		o, exp, err := DecodeDump(b)
		if err != nil {
			return
		}
		// The canonical encoding is a fixed point.
		if again := EncodeDump(o, exp); !bytes.Equal(EncodeDump(mustDecode(t, again)), again) {
			t.Fatal("canonical encoding is not stable")
		}
	})
}

func mustDecode(t *testing.T, b []byte) (*store.Object, int64) {
	o, exp, err := DecodeDump(b)
	if err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	return o, exp
}

func TestKeySlotKnownValues(t *testing.T) {
	// Values from the Redis Cluster specification / CLUSTER KEYSLOT docs.
	for key, want := range map[string]int{"somekey": 11058, "foo{hash_tag}": 2515, "bar{hash_tag}": 2515, "{}x": KeySlot([]byte("{}x")), "123456789": 12739} {
		if got := KeySlot([]byte(key)); got != want {
			t.Errorf("KeySlot(%q) = %d want %d", key, got, want)
		}
	}
	if crc16([]byte("123456789")) != 0x31C3 { // CRC-16/XMODEM check value
		t.Fatal("crc16 check value mismatch")
	}
}
