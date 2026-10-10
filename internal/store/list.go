package store

// List is a double-ended queue on a power-of-two ring buffer: O(1) push/pop at both
// ends and O(1) indexed access.
type List struct {
	buf  [][]byte
	head int
	n    int
}

func (l *List) Len() int { return l.n }

func (l *List) idx(i int) int { return (l.head + i) & (len(l.buf) - 1) }

func (l *List) grow() {
	if l.n < len(l.buf) {
		return
	}
	nb := make([][]byte, max(4, 2*len(l.buf)))
	for i := 0; i < l.n; i++ {
		nb[i] = l.buf[l.idx(i)]
	}
	l.buf, l.head = nb, 0
}

func (l *List) PushFront(v []byte) {
	l.grow()
	l.head = (l.head - 1) & (len(l.buf) - 1)
	l.buf[l.head] = v
	l.n++
}

func (l *List) PushBack(v []byte) {
	l.grow()
	l.buf[l.idx(l.n)] = v
	l.n++
}

// PopFront removes the first element. The list must not be empty.
func (l *List) PopFront() []byte {
	v := l.buf[l.head]
	l.buf[l.head] = nil
	l.head = (l.head + 1) & (len(l.buf) - 1)
	l.n--
	return v
}

// PopBack removes the last element. The list must not be empty.
func (l *List) PopBack() []byte {
	i := l.idx(l.n - 1)
	v := l.buf[i]
	l.buf[i] = nil
	l.n--
	return v
}

// At returns element i, 0 <= i < Len().
func (l *List) At(i int) []byte { return l.buf[l.idx(i)] }

func (l *List) SetAt(i int, v []byte) { l.buf[l.idx(i)] = v }

// Range returns elements start..stop inclusive (already normalized).
func (l *List) Range(start, stop int) [][]byte {
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, l.At(i))
	}
	return out
}

// Trim keeps only start..stop inclusive (already normalized); empty if start > stop.
func (l *List) Trim(start, stop int) {
	var keep [][]byte
	if start <= stop {
		keep = l.Range(start, stop)
	}
	l.rebuild(keep)
}

// Remove deletes up to |count| elements equal to v: from the head if count > 0, from
// the tail if count < 0, all if count == 0. Returns the number removed.
func (l *List) Remove(count int, v []byte) int {
	removed := 0
	keep := make([][]byte, 0, l.n)
	limit := count
	if limit < 0 {
		limit = -limit
	}
	match := func(e []byte) bool {
		return (limit == 0 || removed < limit) && string(e) == string(v)
	}
	if count >= 0 {
		for i := 0; i < l.n; i++ {
			if e := l.At(i); match(e) {
				removed++
			} else {
				keep = append(keep, e)
			}
		}
	} else {
		for i := l.n - 1; i >= 0; i-- {
			if e := l.At(i); match(e) {
				removed++
			} else {
				keep = append(keep, e)
			}
		}
		for i, j := 0, len(keep)-1; i < j; i, j = i+1, j-1 {
			keep[i], keep[j] = keep[j], keep[i]
		}
	}
	if removed > 0 {
		l.rebuild(keep)
	}
	return removed
}

func (l *List) rebuild(elems [][]byte) {
	*l = List{}
	for _, e := range elems {
		l.PushBack(e)
	}
}

func (l *List) clone() *List {
	c := &List{}
	for i := 0; i < l.n; i++ {
		c.PushBack(l.At(i))
	}
	return c
}
