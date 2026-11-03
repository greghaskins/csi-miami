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

// oldest returns the least recently used chunk.
func (l *lru) oldest() (uint32, bool) {
	e := l.order.Back()
	if e == nil {
		return 0, false
	}
	return e.Value.(uint32), true
}
