package main

import (
	"sync"
	"time"
)

type item struct {
	value  any
	expiry time.Time
}
type store struct {
	mu       sync.Mutex
	entries  map[string]item
	stoppedC chan struct{}
}

func newStore() *store {
	return &store{
		entries:  map[string]item{},
		stoppedC: make(chan struct{}, 1),
	}
}

func (st *store) init() {
	timer := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-st.stoppedC:
			return

		case <-timer.C:
			for key, entry := range st.entries {
				if entry.expiry.IsZero() {
					continue
				}

				if entry.expiry.Before(time.Now()) {
					st.removeItem(key)
				}
			}
		}
	}

}

func (st *store) getItem(key string) any {
	st.mu.Lock()
	defer st.mu.Unlock()

	item, ok := st.entries[key]

	if !ok {
		return nil
	}

	if item.expiry.IsZero() {
		return item.value
	}

	if item.expiry.Before(time.Now()) {
		delete(st.entries, key)
		return nil
	}

	return item.value
}

func (st *store) setItem(key string, value any, expiry time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.entries[key] = item{value: value, expiry: expiry}
}

func (st *store) removeItem(key string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.entries, key)
}

func (st *store) shutdown() {
	close(st.stoppedC)
}
