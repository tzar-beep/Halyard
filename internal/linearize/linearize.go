// Package linearize checks whether a concurrent history of key-value operations is
// linearizable: whether every operation can be assigned an instant between its
// call and its return such that the resulting sequential history is legal for a
// register per key.
//
// It implements the Wing & Gong search with Lowe's memoization (the algorithm
// behind Porcupine and Knossos): repeatedly pick an operation that may take effect
// next (its call precedes every pending return), apply it to the model, and
// backtrack on dead ends, caching (linearized set, model state) pairs already
// explored. Keys are independent registers, so each key is checked separately.
//
// Operations whose outcome is unknown (client timeout, dropped connection) get
// Return = Unknown: they may have taken effect at any point after their call, or
// never.
package linearize

import (
	"math"
	"sort"
	"strings"
)

const Unknown = math.MaxInt64

// Op is one client operation. Value is what was written, or what a read returned
// (Nil for a missing key).
type Op struct {
	Client       int
	Key          string
	Write        bool
	Value        string
	Call, Return int64 // timestamps from one monotonic clock
}

// Nil is the read result for a missing key, distinct from any written value.
const Nil = "\x00nil"

// Result reports the first key whose history is not linearizable.
type Result struct {
	OK  bool
	Key string
	Ops []Op // that key's history, for diagnosis
}

// Check verifies every key's history.
func Check(ops []Op) Result {
	// Written values are unique per key, which permits two sound reductions for
	// writes of unknown outcome (otherwise they stay "pending" forever and the
	// search space explodes):
	//   - never read: drop it. If a linearization exists with it, deleting it
	//     leaves one, since no read depends on its value.
	//   - read: it happened, no later than the first read that saw it returned.
	firstSeen := map[[2]string]int64{}
	for _, op := range ops {
		if !op.Write && op.Return != Unknown {
			k := [2]string{op.Key, op.Value}
			if t, ok := firstSeen[k]; !ok || op.Return < t {
				firstSeen[k] = op.Return
			}
		}
	}
	byKey := map[string][]Op{}
	for _, op := range ops {
		if op.Return == Unknown {
			if !op.Write {
				continue // an unfinished read constrains nothing
			}
			t, seen := firstSeen[[2]string{op.Key, op.Value}]
			if !seen {
				continue
			}
			op.Return = max(t, op.Call)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			return Result{OK: false, Key: k, Ops: byKey[k]}
		}
	}
	return Result{OK: true}
}

type entry struct {
	id         int
	call       bool
	t          int64
	prev, next *entry
	match      *entry
}

func checkKey(ops []Op) bool {
	n := len(ops)
	events := make([]*entry, 0, 2*n)
	for i, op := range ops {
		c := &entry{id: i, call: true, t: op.Call}
		r := &entry{id: i, t: op.Return}
		c.match = r
		events = append(events, c, r)
	}
	// Calls sort before returns at equal times: treating touching operations as
	// concurrent can only accept more histories, never reject a valid one.
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].t != events[j].t {
			return events[i].t < events[j].t
		}
		return events[i].call && !events[j].call
	})
	head := &entry{}
	prev := head
	for _, e := range events {
		prev.next, e.prev = e, prev
		prev = e
	}

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		m := e.match
		m.prev.next = m.next
		if m.next != nil {
			m.next.prev = m.prev
		}
	}
	unlift := func(e *entry) {
		m := e.match
		m.prev.next = m
		if m.next != nil {
			m.next.prev = m
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e         *entry
		prevState string
	}
	linearized := make([]byte, (n+7)/8)
	memo := map[string]bool{}
	var stack []frame
	state := Nil
	e := head.next
	for head.next != nil {
		if e == nil {
			return true
		}
		if !e.call {
			if ops[e.id].Return == Unknown {
				// Only operations that may never have happened remain.
				return true
			}
			if len(stack) == 0 {
				return false
			}
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			linearized[top.e.id/8] &^= 1 << (top.e.id % 8)
			state = top.prevState
			unlift(top.e)
			e = top.e.next
			continue
		}
		op := ops[e.id]
		newState, legal := state, true
		if op.Write {
			newState = op.Value
		} else {
			legal = state == op.Value
		}
		if legal {
			linearized[e.id/8] |= 1 << (e.id % 8)
			var key strings.Builder
			key.Write(linearized)
			key.WriteByte(0)
			key.WriteString(newState)
			if !memo[key.String()] {
				memo[key.String()] = true
				stack = append(stack, frame{e, state})
				state = newState
				lift(e)
				e = head.next
				continue
			}
			linearized[e.id/8] &^= 1 << (e.id % 8)
		}
		e = e.next
	}
	return true
}
