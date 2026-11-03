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
type Judgement struct {
	Takeover bool
	Reason   string
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
// info is on the server's clock; a not Ready node needs the taker's own
// observation to agree with it before any timed takeover (the two-clock rule),
// so that a skewed or stepped clock cannot make every live holder look stale
// at once. A holder that is not present is trivially acquirable and the
// verdict is not a takeover.
func Judge(info Info, node NodeState, takeoverTimeout time.Duration, observed Observation) Judgement {
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
		switch {
		case node.InUse:
			return no("the holder's node is Ready and still reports the volume in use")
		case info.Age < StaleAfter:
			return no("the holder's node is Ready but the holder is %s old, under %s", info.Age, StaleAfter)
		}
		return Judgement{Takeover: true, Reason: fmt.Sprintf("the holder's node is Ready, does not report the volume in use and the holder is %s old", info.Age)}
	}
	switch {
	case info.Age < takeoverTimeout:
		return no("the holder's node is not Ready but the holder is %s old on the server's clock, under %s", info.Age, takeoverTimeout)
	case observed.RenewalsUnchangedFor < takeoverTimeout:
		return no("the holder is %s old on the server's clock but its renewals changed %s ago on ours, under %s", info.Age, observed.RenewalsUnchangedFor, takeoverTimeout)
	}
	return Judgement{Takeover: true, Reason: fmt.Sprintf("the holder's node is not Ready and the holder is %s old on the server's clock and unrenewed for %s on ours", info.Age, observed.RenewalsUnchangedFor)}
}
