package holder

import "fmt"

// Judgement is the reconciler's verdict on a holder it found: whether the
// holder may be replaced, and why or why not. Acquire consumes it and never
// computes it.
//
// There is no timed takeover (decision 52): a holder is replaced only by the
// emergency detach, an operator's act, and the reconciler's judge
// (internal/reconcile) is the one place that decides it. Nothing here reads a
// holder's age.
//
// Info is the holder that was judged: Acquire re-reads the file and refuses the
// takeover unless the holder it finds is still that one (same node and
// epoch), so a takeover by a third node between the judgement and the break is
// not overwritten.
type Judgement struct {
	Takeover bool
	Reason   string
	Info     Info
}

// DetachValue is the value of the emergency-detach annotation that names this
// holder: "<node>@<epoch>". It is empty when no holder is present.
func (i Info) DetachValue() string {
	if !i.Present {
		return ""
	}
	return fmt.Sprintf("%s@%d", i.Node, i.Epoch)
}

// DetachMatches reports whether an annotation value names exactly this holder,
// as read now. The comparison is byte for byte: a value naming an older epoch,
// another node, or the right pair with any padding does not match, so an
// annotation left in place after a takeover names a holder that is gone and
// can never take over its successor.
func (i Info) DetachMatches(value string) bool {
	return i.Present && value == i.DetachValue()
}
