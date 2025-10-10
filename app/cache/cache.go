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
	items map[string]item
	mu    sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		items: map[string]item{},
	}
}

func (i *item) GetTTL() time.Time {
	return i.expiry
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

func (ch *Cache) GetItems() map[string]item {
	return ch.items
}

func (ch *Cache) SetItem(key string, value any, expiry time.Time) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.items[key] = item{value: value, expiry: expiry}
}

func (ch *Cache) Size() int {
	return len(ch.items)
}

func (ch *Cache) RemoveItem(key string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	delete(ch.items, key)
}
