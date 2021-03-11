package influx

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newSimulationDataSource(sim common.Simulator) targets.DataSource {
	return &simulationDataSource{
		simulator: sim,
		headers:   sim.Headers(),
	}
}

type simulationDataSource struct {
	simulator common.Simulator
	headers   *common.GeneratedDataHeaders
}

func (s simulationDataSource) NextItem() data.LoadedPoint {
	p := data.NewPoint()
	var write bool
	for !s.simulator.Finished() {
		write = s.simulator.Next(p)
		if write {
			break
		}
		p.Reset()
	}
	if s.simulator.Finished() || !write {
		return data.LoadedPoint{}
	}

	//writer := bufio.NewWriter(buf)
	serializer := Serializer{}
	buffer := bytes.Buffer{}
	err := serializer.Serialize(p, &buffer)
	if err != nil {
		fatal("parse point failed")
	}
	return data.NewLoadedPoint(buffer.Bytes())
}

func (s simulationDataSource) Headers() *common.GeneratedDataHeaders {
	return s.simulator.Headers()
}
