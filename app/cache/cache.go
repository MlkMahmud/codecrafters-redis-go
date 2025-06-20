package cache

import (
	"sync"
	"time"
)

type item struct {
	value  any
	expiry time.Time
}

type Cache struct {
	hz              int
	items           map[string]item
	mu              sync.Mutex
	cleanerStoppedC chan struct{}
}

type CacheConfig struct {
	// Controls the frequency (in milliseconds) at which the cache performs background tasks like expiring keys.
	HZ int
}

func NewCache(cfg CacheConfig) *Cache {
	var hz = cfg.HZ

	if hz == 0 {
		hz = 5000
	}

	return &Cache{
		hz:              hz,
		items:           map[string]item{},
		cleanerStoppedC: make(chan struct{}, 1),
	}
}

func (ch *Cache) GetItem(key string) any {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	item, ok := ch.items[key]

	if !ok {
		return nil
	}

	if item.expiry.IsZero() {
		return item.value
	}

	if item.expiry.Before(time.Now()) {
		delete(ch.items, key)
		return nil
	}

	return item.value
}

func (ch *Cache) SetItem(key string, value any, expiry time.Time) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.items[key] = item{value: value, expiry: expiry}
}

func (ch *Cache) RemoveItem(key string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	delete(ch.items, key)
}

func (ch *Cache) StartCacheCleaner() {
	duration := time.Duration(ch.hz) * time.Millisecond
	timer := time.NewTicker(duration)

	for {
		select {
		case <-ch.cleanerStoppedC:
			return

		case <-timer.C:
			for key, entry := range ch.items {
				if entry.expiry.IsZero() {
					continue
				}

				if entry.expiry.Before(time.Now()) {
					ch.RemoveItem(key)
				}
			}
		}
	}

}

func (ch *Cache) StopCacheCleaner() {
	close(ch.cleanerStoppedC)
}
