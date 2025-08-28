package importerV2

type tableCache struct {
	seq  int32
	vMap map[string]int32 // value | sequence
}

// Returns the next sequence and a bool that represents if the value was new
func (c *tableCache) NewEntry(value string) (int32, bool) {
	if value == "" {
		return 0, false
	}

	if c.vMap == nil {
		c.vMap = make(map[string]int32)
	}

	seq, ok := c.vMap[value]
	if !ok {
		c.seq++
		c.vMap[value] = c.seq

		// new value
		return c.seq, true
	}

	// value already exists
	return seq, false
}
