package file

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// resources collects cpu and memory stats and prints them.
type resources struct {
	maxAlloc uint64
	maxSys   uint64
	numGc    uint32
}

// ResourceCollector returns an object that would collect system allocations
// and number of garbage collections in the provided intervals. You should
// cancel the context once you are done with this object to release the
// associated resources.
func ResourceCollector(ctx context.Context, d time.Duration) fmt.Stringer {
	r := &resources{}
	ticker := time.NewTicker(d)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				r.collect()
			}
		}
	}()
	return r
}

func (r *resources) collect() {
	var s runtime.MemStats
	runtime.ReadMemStats(&s)
	if s.Alloc > r.maxAlloc {
		r.maxAlloc = s.Alloc
	}
	if s.Sys > r.maxSys {
		r.maxSys = s.Sys
	}

	r.numGc = s.NumGC
}

func (r *resources) String() string {
	str := &strings.Builder{}
	fmt.Fprintf(str, "Max Alloc = %v MiB", bToMb(r.maxAlloc))
	fmt.Fprintf(str, "\tMax Sys = %v MiB", bToMb(r.maxSys))
	fmt.Fprintf(str, "\tNumGC = %v\n", r.numGc)
	return str.String()
}
