package pandat

import (
	"strconv"
)

func ReadMap(data map[string][]string) *DataFrame[any] {
	seriess := make([]*Series[any], 0, len(data))
	for name, values := range data {
		seriess = append(seriess, NewSeries(name, detectType(values)...))
	}
	return NewDataFrame(seriess...)
}

func ReadSlice(arr [][]string, hasHeader bool) *DataFrame[any] {
	seriess := make([]*Series[any], 0, len(arr))
	for i, values := range arr {
		if hasHeader {
			seriess = append(seriess, NewSeries[any](values[0], detectType(values[1:])...))
		} else {
			seriess = append(seriess, NewSeries[any](strconv.Itoa(i), detectType(values)...))
		}
	}
	return NewDataFrame(seriess...)
}

func detectType(arr []string) []any {
	values := make([]any, 0, len(arr))
	for _, val := range arr {
		if val == "" || val == "NaN" {
			values = append(values, nil)
			continue
		}
		if val == "true" || val == "True" {
			values = append(values, true)
			continue
		}
		if val == "false" || val == "False" {
			values = append(values, false)
			continue
		}
		if val, err := strconv.ParseInt(val, 10, 64); err == nil {
			values = append(values, val)
			continue
		}

		if val, err := strconv.ParseFloat(val, 64); err == nil {
			values = append(values, val)
			continue
		}

		values = append(values, val)
	}
	return values
}
