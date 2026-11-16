package volume

import (
	"slices"
	"strconv"
	"testing"
)

// The block server keeps the class's generations.keep: the unit a stage
// starts carries it from the volume context, so a restage keeps the class's
// value. (The volume parameters refuse a keep below 1.)
func TestUnitCarriesTheClassGenerationsKeep(t *testing.T) {
	for _, c := range []struct{ keep, want int }{{1, 1}, {3, 3}, {10, 10}} {
		t.Run(strconv.Itoa(c.keep), func(t *testing.T) {
			e := newEnv(t)
			e.in.Volume.Generations.Keep = c.keep
			e.newItem()
			e.mustConverge()
			if len(e.sd.units) == 0 {
				t.Fatal("no unit was started")
			}
			args := e.sd.units[0].ExecStart
			i := slices.Index(args, "-keep")
			if i < 0 || i+1 >= len(args) {
				t.Fatalf("the unit has no -keep: %v", args)
			}
			if got, _ := strconv.Atoi(args[i+1]); got != c.want {
				t.Errorf("-keep %s, want %d", args[i+1], c.want)
			}
		})
	}
}
