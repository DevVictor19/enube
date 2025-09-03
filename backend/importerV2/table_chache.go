package importerV2

import (
	"strconv"
)

type tableCache struct {
	seq  int
	vMap map[string]int // value | sequence

}

func newTableCache() *tableCache {
	return &tableCache{
		vMap: make(map[string]int),
	}
}

// Returns the corresponding sequence and a bool value indicating if the value was new
func (c *tableCache) NewEntry(value string) (string, bool) {
	if value == "" {
		return "0", false
	}

	seq, ok := c.vMap[value]
	if !ok {
		c.seq++
		seq = c.seq
		c.vMap[value] = seq
		return strconv.Itoa(seq), true
	}

	return strconv.Itoa(seq), false
}
