package importerV2

import "sync"

type tableCache struct {
	seq  int32
	vMap map[string]int32 // value | sequence
	mu   sync.RWMutex
}

func newTableCache() *tableCache {
	return &tableCache{
		vMap: make(map[string]int32),
	}
}

// Returns the sequence and bool variable that represents if the value exists.
//
// If no value was found in cache, the sequence returned will be 0.
func (c *tableCache) Get(value string) (int32, bool) {
	if value == "" {
		return 0, false
	}

	defer c.mu.RUnlock()
	c.mu.RLock()

	seq, ok := c.vMap[value]
	if !ok {
		return 0, false
	}

	return seq, true
}

// Sets a new value in cache and returns the new sequence.
func (c *tableCache) Set(value string) int32 {
	defer c.mu.Unlock()
	c.mu.Lock()

	new := c.seq + 1
	c.vMap[value] = new
	c.seq = new

	return new
}
