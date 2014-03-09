// package cache implements a really primitive cache that never clears itself
// out.
package cache

import (
	"sync"
	"time"
)

type Cache struct {
	data       []byte
	expiration time.Time
	mutex      *sync.Mutex
}

func NewCache() *Cache {
	return &Cache{mutex: &sync.Mutex{}}
}

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

func (cache *Cache) Set(data []byte, ttl time.Duration) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	cache.data = data
	cache.expiration = time.Now().Add(ttl)
}
