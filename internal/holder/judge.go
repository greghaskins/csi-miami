package holder

import (
	"fmt"
	"time"
)

// StaleAfter is how long a holder on a Ready node that no longer reports the
// volume in use must have gone without renewing before it can be taken over.
const StaleAfter = 30 * time.Second

// Judgement is the reconciler's verdict on a holder it found: whether the
// holder may be replaced, and why or why not. Acquire consumes it and never
// computes it; Judge is the function the reconciler calls.
//
// Info is the holder Judge judged: Acquire re-reads the file and refuses the
// takeover unless the holder it finds is still exactly this one (same node,
// epoch and renewal count), so a renewal that lands between the judgement and
// the break cannot cost a live holder its volume.
type Judgement struct {
	Takeover bool
	Reason   string
	Info     Info
}

// NodeState describes the holder's node as the API server reports it.
type NodeState struct {
	Ready        bool // the Ready condition is true
	InUse        bool // the volume is still in the node's volumesInUse or volumesAttached
	OutOfService bool // the node carries the out-of-service taint
}

// Observation is the taker's own observation of the holder, made on its own
// monotonic clock: for how long the holder's renewal count has not moved.
type Observation struct {
	RenewalsUnchangedFor time.Duration
}

// Judge applies the takeover conditions to a holder found at info. The age in
// info is on the server's clock; a timed takeover (every takeover but the
// out-of-service one) also needs the taker's own observation to agree that the
// holder has not renewed for the full period on the taker's monotonic clock
// (the two-clock rule), so that a skewed or stepped clock cannot make every
// live holder look stale at once. takeoverTimeout must be positive. A holder
// that is not present is trivially acquirable and the verdict is not a
// takeover.
func Judge(info Info, node NodeState, takeoverTimeout time.Duration, observed Observation) (Judgement, error) {
	if takeoverTimeout <= 0 {
		return Judgement{}, fmt.Errorf("holder: takeoverTimeout must be positive, got %s", takeoverTimeout)
	}
	j := judge(info, node, takeoverTimeout, observed)
	j.Info = info
	return j, nil
}

func judge(info Info, node NodeState, takeoverTimeout time.Duration, observed Observation) Judgement {
	no := func(format string, args ...any) Judgement {
		return Judgement{Reason: fmt.Sprintf(format, args...)}
	}
	if !info.Present {
		return no("no holder")
	}
	if node.OutOfService {
		return Judgement{Takeover: true, Reason: "the holder's node carries the out-of-service taint"}
	}
	if node.Ready {
		// Ready, not in use and stale is a timed takeover too, so it obeys the
		// two-clock rule: the server's clock says the holder is old enough AND
		// the taker's own clock agrees it has not renewed for that long.
		switch {
		case node.InUse:
			return no("the holder's node is Ready and still reports the volume in use")
		case info.Age < StaleAfter:
			return no("the holder's node is Ready but the holder is %s old on the server's clock, under %s", info.Age, StaleAfter)
		case observed.RenewalsUnchangedFor < StaleAfter:
			return no("the holder's node is Ready and the holder is %s old on the server's clock but its renewals changed %s ago on ours, under %s", info.Age, observed.RenewalsUnchangedFor, StaleAfter)
		}
		return Judgement{Takeover: true, Reason: fmt.Sprintf("the holder's node is Ready, does not report the volume in use, is %s old on the server's clock and unrenewed for %s on ours", info.Age, observed.RenewalsUnchangedFor)}
	}
	switch {
	case info.Age < takeoverTimeout:
		return no("the holder's node is not Ready but the holder is %s old on the server's clock, under %s", info.Age, takeoverTimeout)
	case observed.RenewalsUnchangedFor < takeoverTimeout:
		return no("the holder is %s old on the server's clock but its renewals changed %s ago on ours, under %s", info.Age, observed.RenewalsUnchangedFor, takeoverTimeout)
	}
	return Judgement{Takeover: true, Reason: fmt.Sprintf("the holder's node is not Ready and the holder is %s old on the server's clock and unrenewed for %s on ours", info.Age, observed.RenewalsUnchangedFor)}
}
