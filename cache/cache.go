// package cache implements a really primitive cache that never clears itself
// out.
package cache

import (
	"sync"
	"time"
)

// Cache is a cache for binary data
type Cache struct {
	data       []byte
	expiration time.Time
	mutex      *sync.Mutex
}

// NewCache constructs a Cache
func NewCache() *Cache {
	return &Cache{mutex: &sync.Mutex{}}
}

// Get returns the currently cached value, as long as it hasn't expired.
func (cache *Cache) Get() []byte {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if cache.data == nil {
		return nil
	} else if cache.expiration.Before(time.Now()) {
		return nil
	} else {
		return cache.data
	}
}

// Set sets a value in the cache with an expiration of now + ttl.
func (cache *Cache) Set(data []byte, ttl time.Duration) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	cache.data = data
	cache.expiration = time.Now().Add(ttl)
}
