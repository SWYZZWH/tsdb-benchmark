package open_telemetry

import (
	"github.com/timescale/tsbs/pkg/data"
	"hash/fnv"
)

func newPointIndexer(maxPartitions uint) *pointIndexer {
	return &pointIndexer{Partitions: maxPartitions}
}

type pointIndexer struct {
	Partitions uint
}

func (self *pointIndexer) GetIndex(p data.LoadedPoint) uint {
	tagstr := p.Data.(*point).row.tags
	h := fnv.New32()
	hashcode, _ := h.Write([]byte(tagstr))
	return uint(hashcode) % self.Partitions
}
