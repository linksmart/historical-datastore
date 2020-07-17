//Package senmltest implements senml testing utilities
package data

import (
	"math"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/registry"
)

func Same_name_same_types(count int, series registry.TimeSeries, decremental bool) senml.Pack {
	value := 22.1
	stringValue := "Machine Room"
	boolValue := false
	dataValue := "aGkgCg"
	timeinit := 1543059346.0
	mult := 1.0
	if decremental == true {
		timeinit = timeinit + float64(count-1)
		mult = -1.0
	}

	var s = make([]senml.Record, count)

	switch series.Type {
	case registry.Float:
		s[0] = senml.Record{BaseName: series.Name,
			BaseUnit: series.Unit,
			Value:    &value, Time: timeinit}
		for i := 1; i < count; i++ {
			s[i] = senml.Record{Value: &value, Time: (timeinit + float64(i)*mult)}
		}
	case registry.String:
		s[0] = senml.Record{BaseName: series.Name,
			BaseUnit:    series.Unit,
			StringValue: stringValue, Time: timeinit}
		for i := 1; i < count; i++ {
			s[i] = senml.Record{StringValue: stringValue, Time: (timeinit + float64(i)*mult)}
		}
	case registry.Bool:
		s[0] = senml.Record{BaseName: series.Name,
			BaseUnit:  series.Unit,
			BoolValue: &boolValue, Time: timeinit}
		for i := 1; i < count; i++ {
			s[i] = senml.Record{BoolValue: &boolValue, Time: (timeinit + float64(i)*mult)}
		}
	case registry.Data:
		s[0] = senml.Record{BaseName: series.Name,
			BaseUnit:  series.Unit,
			DataValue: dataValue, Time: timeinit}
		for i := 1; i < count; i++ {
			s[i] = senml.Record{DataValue: dataValue, Time: (timeinit + float64(i)*mult)}
		}
	}

	return s
}

func CompareRecords(r1 senml.Record, r2 senml.Record) (same bool) {
	return math.Abs(r1.Time-r2.Time) < 1e-6 &&
		r1.Name == r2.Name &&
		r1.DataValue == r2.DataValue &&
		r1.StringValue == r2.StringValue &&
		((r1.Sum == nil && r2.Sum == nil) || *r1.Sum == *r2.Sum) &&
		((r1.BoolValue == nil && r2.BoolValue == nil) || *r1.BoolValue == *r2.BoolValue) &&
		((r1.Value == nil && r2.Value == nil) || *r1.Value == *r2.Value)
}

func CompareSenml(s1 senml.Pack, s2 senml.Pack) (same bool) {
	recordLen := len(s1)
	for i := 0; i < recordLen; i++ {
		r1 := s1[i]
		r2 := s2[i]
		if CompareRecords(r1, r2) == false {
			return false
		}
	}
	return true
}

func Diff_name_diff_types() senml.Pack {

	value := 22.1
	sum := 0.0
	vb := true

	var s = []senml.Record{
		{BaseName: "dev123",
			BaseTime: -45.67,
			BaseUnit: "degC",
			Value:    &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
		{StringValue: "kitchen", Name: "room", Time: -1.0},
		{DataValue: "abc", Name: "data"},
		{BoolValue: &vb, Name: "ok"},
	}
	return s
}

type aggrFunction func(pack senml.Pack) senml.Record

func avg(pack senml.Pack) senml.Record {
	sum := 0.0
	name := pack[0].Name
	for _, record := range pack {
		sum += *record.Value
	}
	avg := sum / float64(len(pack))
	return senml.Record{Name: name, Value: &avg, Time: pack[len(pack)-1].Time}
}
func sampleDataForAggregation(maxPerBlock int,
	series registry.TimeSeries,
	from float64,
	to float64,
	aggrFunc aggrFunction,
	interval time.Duration) (rawPack senml.Pack, aggrPack senml.Pack) {

	curVal := 0.0

	durSec := to - from
	intervalDurSec := interval.Seconds()
	increment := durSec / (intervalDurSec * float64(maxPerBlock))
	totalBlocks := int(durSec / intervalDurSec)
	totalCount := totalBlocks * maxPerBlock
	rawPack = make([]senml.Record, 0, totalCount)
	aggrPack = make([]senml.Record, 0, totalBlocks)

	for curTime := from; curTime < to; curTime += intervalDurSec {
		curPack := make([]senml.Record, 0, maxPerBlock)
		for i := curTime; i < curTime+intervalDurSec; i += increment {
			value := curVal
			record := senml.Record{Name: series.Name, Value: &value, Time: (i)}
			curVal++
			curPack = append(curPack, record)
		}
		rawPack = append(rawPack, curPack...)
		aggrPack = append(aggrPack, aggrFunc(curPack))
	}

	return rawPack, aggrPack

}
