package pandat

import (
	"math"
	"reflect"
	"strconv"
)

func ReadMap(data map[string][]string) *DataFrame[any] {
	seriess := make([]*Series[any], 0, len(data))
	for name, values := range data {
		seriess = append(seriess, NewSeries(name, asType(values, determineType(values))...))
	}
	return NewDataFrame(seriess...)
}

func ReadSlice(arr [][]string, hasHeader bool) *DataFrame[any] {
	seriess := make([]*Series[any], 0, len(arr))
	for i, values := range arr {
		if hasHeader {
			header := values[0]
			values = values[1:]
			seriess = append(seriess, NewSeries[any](header, asType(values, determineType(values))...))
		} else {
			seriess = append(seriess, NewSeries[any](strconv.Itoa(i), asType(values, determineType(values))...))
		}
	}
	return NewDataFrame(seriess...)
}

func determineType(arr []string) reflect.Kind {
	var (
		hasBool, hasFloat, hasInt, hasOthers bool
	)
	for _, val := range arr {
		if val == "" {
			continue
		}
		if val == "true" || val == "True" || val == "false" || val == "False" {
			hasBool = true
			continue
		}
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			hasInt = true
			continue
		}
		if val == "NaN" {
			hasFloat = true
			continue
		}
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			hasFloat = true
			continue
		}

		hasOthers = true
		break // fast break on string
	}

	// mixed dtype, each value has its own go type
	if hasOthers {
		return reflect.Interface
	}

	// float64 if has float values or has both float and int values and has not bool
	if hasFloat && !hasBool {
		return reflect.Float64
	}

	// int64 if has only int values
	if hasInt && !hasBool {
		return reflect.Int64
	}

	// bool if has only bool values
	if hasBool && !hasFloat && !hasInt {
		return reflect.Bool
	}

	// otherwise, default interface
	return reflect.Interface
}

func asType(arr []string, dtype reflect.Kind) []any {
	switch dtype {
	case reflect.Float64:
		return asFloat64(arr)
	case reflect.Int64:
		return asInt64(arr)
	case reflect.Bool:
		return asBool(arr)
	case reflect.Interface:
		return asInterface(arr)
	default:
		panic("Unsupported type")
	}
}

func asFloat64(arr []string) []any {
	values := make([]any, 0, len(arr))

	for _, val := range arr {
		switch val {
		case "", "NaN", "None", "null":
			values = append(values, math.NaN())
		case "Inf", "inf":
			values = append(values, math.Inf(1))
		case "-Inf", "-inf":
			values = append(values, math.Inf(-1))
		default:
			if v, err := strconv.ParseFloat(val, 64); err != nil {
				panic(err)
			} else {
				values = append(values, v)
			}
		}
	}

	return values
}

func asInt64(arr []string) []any {
	values := make([]any, 0, len(arr))
	for _, val := range arr {
		if val == "" {
			values = append(values, nil)
		} else if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			values = append(values, v)
		} else {
			panic(err)
		}
	}

	return values
}

func asBool(arr []string) []any {
	values := make([]any, 0, len(arr))
	for _, val := range arr {
		switch val {
		case "True", "true":
			values = append(values, true)
		case "False", "false":
			values = append(values, false)
		case "":
			values = append(values, nil)
		default:
			panic("not a bool value: " + val)
		}
	}

	return values
}

func asInterface(arr []string) []any {
	values := make([]any, 0, len(arr))
	for _, val := range arr {
		if val == "" || val == "NaN" {
			values = append(values, math.NaN())
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

		// default is string
		values = append(values, val)
	}
	return values
}
