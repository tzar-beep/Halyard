//go:build js && wasm

// Command playground exposes the in-process Raft cluster simulator to JavaScript.
// Build: GOOS=js GOARCH=wasm go build -o web/raft.wasm ./cmd/playground
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/tzar-beep/redis/internal/playground"
)

var sim *playground.Sim

func main() {
	api := map[string]any{
		"create": js.FuncOf(func(_ js.Value, a []js.Value) any {
			sim = playground.New(a[0].Int(), uint64(a[1].Int()))
			return nil
		}),
		"step": js.FuncOf(func(_ js.Value, a []js.Value) any {
			for n := a[0].Int(); n > 0; n-- {
				sim.Step()
			}
			return nil
		}),
		"view": js.FuncOf(func(js.Value, []js.Value) any {
			b, _ := json.Marshal(sim.View())
			return string(b)
		}),
		"propose": js.FuncOf(func(_ js.Value, a []js.Value) any { return sim.Propose(a[0].String()) }),
		"crash":   js.FuncOf(func(_ js.Value, a []js.Value) any { sim.Crash(a[0].Int()); return nil }),
		"restart": js.FuncOf(func(_ js.Value, a []js.Value) any { sim.Restart(a[0].Int()); return nil }),
		"isolate": js.FuncOf(func(_ js.Value, a []js.Value) any { sim.Isolate(a[0].Int()); return nil }),
		"loss":    js.FuncOf(func(_ js.Value, a []js.Value) any { sim.SetLoss(a[0].Float()); return nil }),
	}
	js.Global().Set("halyardRaft", js.ValueOf(api))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("halyard-ready"))
	select {}
}
