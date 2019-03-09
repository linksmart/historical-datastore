package integration_tests

import (
	"math"
	"strings"

	"github.com/farshidtz/senml"
)

func Same_name_same_types(count int, name string, decremental bool) senml.Pack {

	value := 22.1
	timeinit := 1543059346.0
	mult := 1.0
	if decremental == true {
		timeinit = timeinit + float64(count-1)
		mult = -1.0
	}

	var s = []senml.Record{
		{BaseName: "urn:dev:ow:10e2073a0108006:" + name,
			BaseUnit:    "A",
			BaseVersion: 5,
			Value:       &value, Name: "current", Time: timeinit},
	}

	for i := 1.0; i < float64(count); i++ {
		s = append(s, senml.Record{Value: &value, Name: "current", Time: (timeinit + i*mult)})
	}
	return s
}

func CompareRecords(r1 senml.Record, r2 senml.Record) bool {
	return (math.Abs(r1.Time-r2.Time) < 1e-6 &&
		strings.Compare(r1.Name, r2.Name) == 0 &&
		strings.Compare(r1.DataValue, r2.DataValue) == 0 &&
		strings.Compare(r1.StringValue, r2.StringValue) == 0 &&
		((r1.Sum == nil && r2.Sum == nil) || *r1.Sum == *r2.Sum) &&
		((r1.BoolValue == nil && r2.BoolValue == nil) || *r1.BoolValue == *r2.BoolValue) &&
		((r1.Value == nil && r2.Value == nil) || *r1.Value == *r2.Value))
}

func CompareSenml(s1 senml.Pack, s2 senml.Pack) bool {
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
			BaseTime:    -45.67,
			BaseUnit:    "degC",
			BaseVersion: 5,
			Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
		{StringValue: "kitchen", Name: "room", Time: -1.0},
		{DataValue: "abc", Name: "data"},
		{BoolValue: &vb, Name: "ok"},
	}
	return s
}
