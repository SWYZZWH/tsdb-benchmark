package prom_pull

import (
	"bufio"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"strings"
)

var fatal = log.Fatalf

const (
	tagsKey = "tags"
)

// for parsing header
func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}

// two lines
type insertData struct {
	tags   string
	fields string
}

// point is a single row of data keyed by which hypertable it belongs
type point struct {
	hypertable string
	row        *insertData
}

// implement of targets.batch
type hypertableArr struct {
	m   map[string][]*insertData
	cnt uint
}

func (ha *hypertableArr) Len() uint {
	return ha.cnt
}

func (ha *hypertableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	k := that.hypertable
	ha.m[k] = append(ha.m[k], that.row)
	ha.cnt++
}

type factory struct{}

func (f *factory) New() targets.Batch {
	newMap := make(map[string][]*insertData)
	return &hypertableArr{
		cnt: 0,
		m:   newMap,
	}
}

func newFileDataSource(fileName string) targets.DataSource {
	br := load.GetBufferedReader(fileName)
	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	// headers are read from the input file, and should be read first
	if d.headers != nil {
		return d.headers
	}
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	var tags string
	var cols []string
	i := 0
	for {
		var line string
		ok := d.scanner.Scan()
		if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
			fatal("ended too soon, no tags or cols read")
			return nil
		} else if !ok {
			fatal("scan error: %v", d.scanner.Err())
			return nil
		}
		if i == 0 {
			tags = d.scanner.Text()
			tags = strings.TrimSpace(tags)
		} else {
			line = d.scanner.Text()
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			cols = append(cols, line)
		}
		i++
	}

	tagsarr := strings.Split(tags, ",")
	if tagsarr[0] != tagsKey {
		fatal("input header in wrong format. got '%s', expected 'tags'", tags[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(tagsarr[1:])
	fieldKeys := make(map[string][]string)
	for _, tableDef := range cols {
		columns := strings.Split(tableDef, ",")
		tableName := columns[0]
		colNames := columns[1:]
		fieldKeys[tableName] = colNames
	}
	d.headers = &common.GeneratedDataHeaders{
		TagTypes:  tagTypes,
		TagKeys:   tagNames,
		FieldKeys: fieldKeys,
	}
	return d.headers
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	if d.headers == nil {
		//try once
		d.headers = d.Headers()
		if d.headers == nil {
			fatal("headers not read before starting to decode points")
			return data.LoadedPoint{}
		}
	}
	newPoint := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}

	// The first line is a CSV line of tags with the first element being "tags"
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsKey {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsKey)
		return data.LoadedPoint{}
	}
	newPoint.tags = parts[1]

	// Scan again to get the data line
	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix = parts[0]
	newPoint.fields = parts[1]

	return data.NewLoadedPoint(&point{
		hypertable: prefix,
		row:        newPoint,
	})
}