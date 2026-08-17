package usecase

import (
	"strconv"
	"sync"
	"time"
)

type cachedKey struct {
	value   []byte
	expires time.Time
}

type keyCache struct {
	mu       sync.Mutex
	capacity int
	values   map[string]cachedKey
}

func newKeyCache(capacity int) *keyCache {
	return &keyCache{capacity: capacity, values: make(map[string]cachedKey)}
}

func (c *keyCache) get(gymID string, version uint64, now time.Time) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheID(gymID, version)
	value, ok := c.values[key]
	if !ok || !now.Before(value.expires) {
		if ok {
			clear(value.value)
			delete(c.values, key)
		}
		return nil, false
	}
	return append([]byte(nil), value.value...), true
}

func (c *keyCache) put(gymID string, version uint64, value []byte, expires time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.values) >= c.capacity {
		for key, existing := range c.values {
			clear(existing.value)
			delete(c.values, key)
			break
		}
	}
	c.values[cacheID(gymID, version)] = cachedKey{value: append([]byte(nil), value...), expires: expires}
}

func (c *keyCache) delete(gymID string, version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheID(gymID, version)
	if value, ok := c.values[key]; ok {
		clear(value.value)
		delete(c.values, key)
	}
}

func cacheID(gymID string, version uint64) string {
	return gymID + ":" + strconv.FormatUint(version, 10)
}
