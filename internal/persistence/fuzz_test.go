package persistence

import (
	"testing"

	"github.com/tzar-beep/redis/internal/store"
)

// FuzzDecodeSnapshot: arbitrary bytes must never panic or allocate without bound,
// and anything accepted must re-encode to an equivalent snapshot.
func FuzzDecodeSnapshot(f *testing.F) {
	z := store.New(store.KindZSet)
	z.ZSet.Add("m", 1.5)
	h := store.New(store.KindHash)
	h.Hash["f"] = []byte("v")
	f.Add(EncodeSnapshot(7, []store.Entry{{Key: "s", Obj: store.NewString([]byte("v"))}, {Key: "z", Obj: z}, {Key: "h", Obj: h, ExpireAt: 1 << 50}}))
	f.Add(EncodeSnapshot(0, nil))
	f.Fuzz(func(t *testing.T, b []byte) {
		var got []store.Entry
		gen, err := DecodeSnapshot(b, func(e store.Entry) { got = append(got, e) })
		if err != nil {
			return
		}
		var again []store.Entry
		if _, err := DecodeSnapshot(EncodeSnapshot(gen, got), func(e store.Entry) { again = append(again, e) }); err != nil || len(again) != len(got) {
			t.Fatalf("re-encode failed: %v (%d vs %d)", err, len(again), len(got))
		}
	})
}
