package store

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fake returns a store with a manual clock and no background expirer.
func fake() (*Store, *atomic.Int64) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	return newStore(clock.Load, false), &clock
}

func TestSetGetDel(t *testing.T) {
	s, _ := fake()
	if _, ok := s.Get("k"); ok {
		t.Fatal("missing key found")
	}
	s.Set("k", []byte("v"), SetOpts{})
	if v, ok := s.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("got %q %v", v, ok)
	}
	if n := s.Del("k", "k", "nope"); n != 1 {
		t.Fatalf("del = %d", n)
	}
}

func TestSetConditions(t *testing.T) {
	s, _ := fake()
	if s.Set("k", []byte("a"), SetOpts{Cond: IfExists}) {
		t.Fatal("XX on missing key succeeded")
	}
	if !s.Set("k", []byte("a"), SetOpts{Cond: IfNotExists}) {
		t.Fatal("NX on missing key failed")
	}
	if s.Set("k", []byte("b"), SetOpts{Cond: IfNotExists}) {
		t.Fatal("NX on existing key succeeded")
	}
}

func TestLazyExpiry(t *testing.T) {
	s, clock := fake()
	s.Set("k", []byte("v"), SetOpts{ExpireAt: clock.Load() + 1000})
	if pttl := s.PTTL("k"); pttl != 1000 {
		t.Fatalf("pttl = %d", pttl)
	}
	clock.Add(999)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("expired early")
	}
	clock.Add(1)
	if _, ok := s.Get("k"); ok {
		t.Fatal("not expired")
	}
	if s.Exists("k") != 0 || s.PTTL("k") != NoKey {
		t.Fatal("expired key visible")
	}
	// NX treats an expired key as missing.
	if !s.Set("k", []byte("new"), SetOpts{Cond: IfNotExists}) {
		t.Fatal("NX blocked by expired key")
	}
	if s.PTTL("k") != NoExpire {
		t.Fatal("old TTL leaked into new key")
	}
}

func TestSetClearsOrKeepsTTL(t *testing.T) {
	s, clock := fake()
	s.Set("k", []byte("v"), SetOpts{ExpireAt: clock.Load() + 5000})
	s.Set("k", []byte("v2"), SetOpts{KeepTTL: true})
	if s.PTTL("k") != 5000 {
		t.Fatal("KEEPTTL lost ttl")
	}
	s.Set("k", []byte("v3"), SetOpts{})
	if s.PTTL("k") != NoExpire {
		t.Fatal("plain SET kept ttl")
	}
}

func TestExpireAndPersist(t *testing.T) {
	s, clock := fake()
	if s.ExpireAt("nope", clock.Load()+10) {
		t.Fatal("expire on missing key")
	}
	s.Set("k", []byte("v"), SetOpts{})
	if s.Persist("k") {
		t.Fatal("persist without ttl returned true")
	}
	s.ExpireAt("k", clock.Load()+10)
	if !s.Persist("k") || s.PTTL("k") != NoExpire {
		t.Fatal("persist failed")
	}
	if !s.ExpireAt("k", clock.Load()-1) || s.Exists("k") != 0 {
		t.Fatal("past expiry should delete")
	}
}

func TestIncr(t *testing.T) {
	s, clock := fake()
	if n, _ := s.IncrBy("c", 1); n != 1 {
		t.Fatalf("got %d", n)
	}
	s.ExpireAt("c", clock.Load()+1000)
	if n, _ := s.IncrBy("c", -5); n != -4 {
		t.Fatalf("got %d", n)
	}
	if s.PTTL("c") != 1000 {
		t.Fatal("INCR dropped TTL")
	}
	s.Set("s", []byte("abc"), SetOpts{})
	if _, err := s.IncrBy("s", 1); err != ErrNotInteger {
		t.Fatalf("err = %v", err)
	}
	s.Set("m", []byte(strconv.FormatInt(math.MaxInt64, 10)), SetOpts{})
	if _, err := s.IncrBy("m", 1); err != ErrOverflow {
		t.Fatalf("err = %v", err)
	}
}

func TestMSetMGet(t *testing.T) {
	s, _ := fake()
	s.MSet([]string{"a", "b"}, [][]byte{[]byte("1"), []byte("2")})
	got := s.MGet([]string{"a", "x", "b"})
	if string(got[0]) != "1" || got[1] != nil || string(got[2]) != "2" {
		t.Fatalf("got %q", got)
	}
}

func TestActiveExpiry(t *testing.T) {
	s := New()
	defer s.Close()
	at := time.Now().UnixMilli() + 50
	for i := 0; i < 1000; i++ {
		s.Set(strconv.Itoa(i), []byte("v"), SetOpts{ExpireAt: at})
	}
	s.Set("keep", []byte("v"), SetOpts{})
	deadline := time.Now().Add(5 * time.Second)
	for s.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active expiry left %d keys", s.Len())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	defer s.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := strconv.Itoa(i % 50)
				s.Set(k, []byte("v"), SetOpts{ExpireAt: time.Now().UnixMilli() + int64(i%3)})
				s.Get(k)
				s.IncrBy("ctr", 1)
				s.MGet([]string{k, "ctr"})
				s.Del(k)
				s.PTTL(k)
			}
		}(g)
	}
	wg.Wait()
	if v, _ := s.Get("ctr"); string(v) != "16000" {
		t.Fatalf("lost increments: ctr = %s", v)
	}
}
