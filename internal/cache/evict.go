package cache

import "container/list"

// lru orders the clean, resident chunks by recency of use. Dirty and
// boundary chunks are never in it: they are pinned until published.
type lru struct {
	order *list.List // front = most recently used; values are uint32
	where map[uint32]*list.Element
}

func newLRU() *lru { return &lru{order: list.New(), where: map[uint32]*list.Element{}} }

func (l *lru) len() int { return len(l.where) }

func (l *lru) has(i uint32) bool { _, ok := l.where[i]; return ok }

// touch adds i or moves it to the most recently used end.
func (l *lru) touch(i uint32) {
	if e, ok := l.where[i]; ok {
		l.order.MoveToFront(e)
		return
	}
	l.where[i] = l.order.PushFront(i)
}

func (l *lru) remove(i uint32) {
	if e, ok := l.where[i]; ok {
		l.order.Remove(e)
		delete(l.where, i)
	}
}

// oldestWhere returns the least recently used chunk that ok accepts.
func (l *lru) oldestWhere(ok func(uint32) bool) (uint32, bool) {
	for e := l.order.Back(); e != nil; e = e.Prev() {
		if i := e.Value.(uint32); ok(i) {
			return i, true
		}
	}
	return 0, false
}
