// Package metrics keeps process-wide counters and histograms and renders them in
// the Prometheus text exposition format. It has no dependencies; hot-path updates
// are atomic adds.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram with fixed exponential buckets (seconds).
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // len(bounds)+1, last is +Inf
	sumNs  atomic.Uint64
	n      atomic.Uint64
}

func NewHistogram(minSec float64, factor float64, buckets int) *Histogram {
	h := &Histogram{counts: make([]atomic.Uint64, buckets+1)}
	b := minSec
	for i := 0; i < buckets; i++ {
		h.bounds = append(h.bounds, b)
		b *= factor
	}
	return h
}

func (h *Histogram) Observe(d time.Duration) {
	s := d.Seconds()
	i := sort.SearchFloat64s(h.bounds, s)
	h.counts[i].Add(1)
	h.sumNs.Add(uint64(d.Nanoseconds()))
	h.n.Add(1)
}

// Quantile estimates a quantile from the buckets (upper bound of the bucket).
func (h *Histogram) Quantile(q float64) time.Duration {
	total := h.n.Load()
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(total)))
	var acc uint64
	for i := range h.counts {
		acc += h.counts[i].Load()
		if acc >= target {
			if i < len(h.bounds) {
				return time.Duration(h.bounds[i] * float64(time.Second))
			}
			return time.Duration(h.bounds[len(h.bounds)-1] * float64(time.Second))
		}
	}
	return 0
}

func (h *Histogram) write(w io.Writer, name, labels string) {
	var acc uint64
	for i, b := range h.bounds {
		acc += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{%sle=\"%g\"} %d\n", name, labels, b, acc)
	}
	acc += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{%sle=\"+Inf\"} %d\n", name, labels, acc)
	lbl := strings.TrimSuffix(labels, ",")
	if lbl != "" {
		lbl = "{" + lbl + "}"
	}
	fmt.Fprintf(w, "%s_sum%s %g\n", name, lbl, float64(h.sumNs.Load())/1e9)
	fmt.Fprintf(w, "%s_count%s %d\n", name, lbl, acc)
}

var (
	cmdMu     sync.RWMutex
	cmdCounts = map[string]*atomic.Uint64{}

	// CommandLatency covers every command from parse to reply buffered.
	CommandLatency = NewHistogram(10e-6, 2, 18) // 10µs .. ~1.3s
	// FsyncLatency covers AOF and Raft WAL fsyncs.
	FsyncLatency = NewHistogram(50e-6, 2, 16)

	ConnectionsCurrent atomic.Int64
	ConnectionsTotal   atomic.Uint64
	ProtocolErrors     atomic.Uint64

	gaugeMu sync.Mutex
	gauges  []func(w io.Writer) // dynamic gauges (Raft state, keyspace size)
	start   = time.Now()
)

// CountCommand increments the per-command counter (name must be lowercase and
// bounded: unknown commands are folded into "unknown").
func CountCommand(name string, known bool) {
	if !known {
		name = "unknown"
	}
	cmdMu.RLock()
	c := cmdCounts[name]
	cmdMu.RUnlock()
	if c == nil {
		cmdMu.Lock()
		if c = cmdCounts[name]; c == nil {
			c = &atomic.Uint64{}
			cmdCounts[name] = c
		}
		cmdMu.Unlock()
	}
	c.Add(1)
}

// CommandCounts returns a copy of the per-command counters.
func CommandCounts() map[string]uint64 {
	cmdMu.RLock()
	defer cmdMu.RUnlock()
	out := make(map[string]uint64, len(cmdCounts))
	for k, v := range cmdCounts {
		out[k] = v.Load()
	}
	return out
}

// RegisterGauges adds a callback that writes gauge lines at scrape time.
func RegisterGauges(fn func(w io.Writer)) {
	gaugeMu.Lock()
	gauges = append(gauges, fn)
	gaugeMu.Unlock()
}

// Uptime since process start.
func Uptime() time.Duration { return time.Since(start) }

// Write renders all metrics in Prometheus text format.
func Write(w io.Writer) {
	fmt.Fprintln(w, "# TYPE redis_commands_total counter")
	counts := CommandCounts()
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "redis_commands_total{cmd=%q} %d\n", n, counts[n])
	}
	fmt.Fprintln(w, "# TYPE redis_command_duration_seconds histogram")
	CommandLatency.write(w, "redis_command_duration_seconds", "")
	fmt.Fprintln(w, "# TYPE redis_fsync_duration_seconds histogram")
	FsyncLatency.write(w, "redis_fsync_duration_seconds", "")
	fmt.Fprintln(w, "# TYPE redis_connections gauge")
	fmt.Fprintf(w, "redis_connections %d\n", ConnectionsCurrent.Load())
	fmt.Fprintln(w, "# TYPE redis_connections_total counter")
	fmt.Fprintf(w, "redis_connections_total %d\n", ConnectionsTotal.Load())
	fmt.Fprintln(w, "# TYPE redis_protocol_errors_total counter")
	fmt.Fprintf(w, "redis_protocol_errors_total %d\n", ProtocolErrors.Load())
	fmt.Fprintln(w, "# TYPE redis_uptime_seconds gauge")
	fmt.Fprintf(w, "redis_uptime_seconds %.0f\n", Uptime().Seconds())
	gaugeMu.Lock()
	fns := append([]func(io.Writer){}, gauges...)
	gaugeMu.Unlock()
	for _, fn := range fns {
		fn(w)
	}
}
