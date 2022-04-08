package pandat

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

type Series[E any] struct {
	name     string
	elements []E
	dtype    reflect.Kind
}

func (s *Series[E]) Name() string {
	return s.name
}

func (s *Series[E]) Rename(name string) *Series[E] {
	return &Series[E]{
		name,
		s.elements,
		s.dtype,
	}
}

func (s *Series[E]) Len() int {
	return len(s.elements)
}

func (s *Series[E]) DType() reflect.Kind {
	if s.dtype != reflect.Invalid {
		return s.dtype
	}
	dtype := reflect.Invalid
	for _, val := range s.elements {
		t := reflect.ValueOf(val).Kind()
		if dtype == reflect.Invalid {
			dtype = t
			continue
		} else if dtype != t {
			dtype = reflect.Interface
			break
		}
	}
	if dtype != reflect.Invalid {
		s.dtype = dtype
	} else {
		dtype = reflect.Interface
	}
	return dtype
}

func (s *Series[E]) Append(inplace bool, vals ...E) *Series[E] {
	if inplace {
		s.elements = append(s.elements, vals...)
		s.dtype = reflect.Invalid
		return s
	}

	return &Series[E]{
		elements: append(s.elements, vals...),
		name:     s.name,
	}
}

func (s *Series[E]) Concat(inplace bool, other *Series[E]) *Series[E] {
	if inplace {
		s.elements = append(s.elements, other.elements...)
		s.dtype = reflect.Invalid
		return s
	}

	return &Series[E]{
		elements: append(s.elements, other.elements...),
		name:     s.name,
	}
}

func (s *Series[E]) AppendAny(vals ...any) *Series[any] {
	elements := make([]any, 0, len(s.elements)+len(vals))
	for _, val := range s.elements {
		elements = append(elements, val)
	}
	elements = append(elements, vals...)
	return &Series[any]{
		elements: vals,
		name:     s.name,
	}
}

func (s *Series[E]) Apply(mapper func(index int, val E) E) *Series[E] {
	elements := make([]E, 0, len(s.elements))
	for i, e := range s.elements {
		elements = append(elements, mapper(i, e))
	}
	return &Series[E]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) ApplyAny(mapper func(index int, val E) any) *Series[any] {
	elements := make([]any, 0, len(s.elements))
	for i, e := range s.elements {
		elements = append(elements, mapper(i, e))
	}
	return &Series[any]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Replace(mapper map[any]any) *Series[any] {
	elements := make([]any, 0, len(s.elements))
	for _, e := range s.elements {
		if replacement, ok := mapper[e]; ok {
			elements = append(elements, replacement)
		} else {
			elements = append(elements, any(e))
		}
	}
	return &Series[any]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) SubSeries(fromIndex, toIndex int) *Series[E] {
	elements := s.elements[fromIndex:toIndex]
	return &Series[E]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) SubSeriesByIndexes(indexes []int) *Series[E] {
	indexer := make(map[int]struct{}, len(indexes))
	for _, i := range indexes {
		indexer[i] = struct{}{}
	}

	return s.SubSeriesByIndexer(indexer)
}

func (s *Series[E]) SubSeriesByIndexer(indexer map[int]struct{}) *Series[E] {
	elements := make([]E, 0, len(indexer))
	for i, e := range s.elements {
		if _, ok := indexer[i]; ok {
			elements = append(elements, e)
		}
	}
	return &Series[E]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Range(fn func(i int, val E)) {
	for i, e := range s.elements {
		fn(i, e)
	}
}

func (s *Series[E]) Slice() []E {
	return s.elements
}

func (s *Series[E]) Filter(filter func(i int, val E) bool) *Series[E] {
	elements := make([]E, 0, len(s.elements)/2)
	for i, e := range s.elements {
		if filter(i, e) {
			elements = append(elements, e)
		}
	}
	return &Series[E]{
		name:     s.name,
		elements: elements,
	}
}

func (s *Series[E]) Min() (float64, bool) {
	return s.ReduceFloat64(math.Min)
}

func (s *Series[E]) Max() (float64, bool) {
	return s.ReduceFloat64(math.Max)
}

func (s *Series[E]) ReduceFloat64(reducer func(left float64, right float64) float64) (float64, bool) {
	if len(s.elements) == 0 {
		return 0.0, false
	}

	var ret float64
	for i, e := range s.Float64().elements {
		if i == 0 {
			ret = e
		} else {
			ret = reducer(ret, e)
		}
	}
	return ret, true
}

func (s *Series[E]) Reduce(reducer func(left E, right E) E) (E, bool) {
	if l := len(s.elements); l == 0 {
		return *new(E), false
	}

	var ret E
	for i, e := range s.elements {
		if i == 0 {
			ret = e
		} else {
			ret = reducer(ret, e)
		}
	}
	return ret, true
}

func (s *Series[E]) Int() *Series[int] {
	elements := make([]int, 0, len(s.elements))
	for _, val := range s.elements {
		switch v := any(val).(type) {
		case uint:
			elements = append(elements, int(v))
		case uint8:
			elements = append(elements, int(v))
		case uint16:
			elements = append(elements, int(v))
		case uint32:
			elements = append(elements, int(v))
		case uint64:
			elements = append(elements, int(v))
		case int:
			elements = append(elements, v)
		case int8:
			elements = append(elements, int(v))
		case int16:
			elements = append(elements, int(v))
		case int32:
			elements = append(elements, int(v))
		case int64:
			elements = append(elements, int(v))
		case float32:
			elements = append(elements, int(v))
		case float64:
			elements = append(elements, int(v))
		case *uint:
			elements = append(elements, int(*v))
		case *uint8:
			elements = append(elements, int(*v))
		case *uint16:
			elements = append(elements, int(*v))
		case *uint32:
			elements = append(elements, int(*v))
		case *uint64:
			elements = append(elements, int(*v))
		case *int:
			elements = append(elements, *v)
		case *int8:
			elements = append(elements, int(*v))
		case *int16:
			elements = append(elements, int(*v))
		case *int32:
			elements = append(elements, int(*v))
		case *int64:
			elements = append(elements, int(*v))
		case *float32:
			elements = append(elements, int(*v))
		case *float64:
			elements = append(elements, int(*v))
		case string:
			if v, err := strconv.Atoi(v); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		case *string:
			if v, err := strconv.Atoi(*v); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		default:
			panic("can not convert to float64")
		}
	}
	return &Series[int]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Int64() *Series[int64] {
	elements := make([]int64, 0, len(s.elements))
	for _, val := range s.elements {
		switch v := any(val).(type) {
		case uint:
			elements = append(elements, int64(v))
		case uint8:
			elements = append(elements, int64(v))
		case uint16:
			elements = append(elements, int64(v))
		case uint32:
			elements = append(elements, int64(v))
		case uint64:
			elements = append(elements, int64(v))
		case int:
			elements = append(elements, int64(v))
		case int8:
			elements = append(elements, int64(v))
		case int16:
			elements = append(elements, int64(v))
		case int32:
			elements = append(elements, int64(v))
		case int64:
			elements = append(elements, v)
		case float32:
			elements = append(elements, int64(v))
		case float64:
			elements = append(elements, int64(v))
		case *uint:
			elements = append(elements, int64(*v))
		case *uint8:
			elements = append(elements, int64(*v))
		case *uint16:
			elements = append(elements, int64(*v))
		case *uint32:
			elements = append(elements, int64(*v))
		case *uint64:
			elements = append(elements, int64(*v))
		case *int:
			elements = append(elements, int64(*v))
		case *int8:
			elements = append(elements, int64(*v))
		case *int16:
			elements = append(elements, int64(*v))
		case *int32:
			elements = append(elements, int64(*v))
		case *int64:
			elements = append(elements, *v)
		case *float32:
			elements = append(elements, int64(*v))
		case *float64:
			elements = append(elements, int64(*v))
		case string:
			if v, err := strconv.ParseInt(v, 10, 64); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		case *string:
			if v, err := strconv.ParseInt(*v, 10, 64); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		default:
			panic("can not convert to float64")
		}
	}
	return &Series[int64]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Float64() *Series[float64] {
	elements := make([]float64, 0, len(s.elements))

	for _, val := range s.elements {
		switch v := any(val).(type) {
		case uint:
			elements = append(elements, float64(v))
		case uint8:
			elements = append(elements, float64(v))
		case uint16:
			elements = append(elements, float64(v))
		case uint32:
			elements = append(elements, float64(v))
		case uint64:
			elements = append(elements, float64(v))
		case int:
			elements = append(elements, float64(v))
		case int8:
			elements = append(elements, float64(v))
		case int16:
			elements = append(elements, float64(v))
		case int32:
			elements = append(elements, float64(v))
		case int64:
			elements = append(elements, float64(v))
		case float32:
			elements = append(elements, float64(v))
		case float64:
			elements = append(elements, v)
		case *uint:
			elements = append(elements, float64(*v))
		case *uint8:
			elements = append(elements, float64(*v))
		case *uint16:
			elements = append(elements, float64(*v))
		case *uint32:
			elements = append(elements, float64(*v))
		case *uint64:
			elements = append(elements, float64(*v))
		case *int:
			elements = append(elements, float64(*v))
		case *int8:
			elements = append(elements, float64(*v))
		case *int16:
			elements = append(elements, float64(*v))
		case *int32:
			elements = append(elements, float64(*v))
		case *int64:
			elements = append(elements, float64(*v))
		case *float32:
			elements = append(elements, float64(*v))
		case *float64:
			elements = append(elements, *v)
		case string:
			if v, err := strconv.ParseFloat(v, 64); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		case *string:
			if v, err := strconv.ParseFloat(*v, 64); err != nil {
				panic(err)
			} else {
				elements = append(elements, v)
			}
		default:
			panic(fmt.Sprintf("can not convert '%s' type '%t' to float64", v, v))
		}
	}
	return &Series[float64]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Str() *Series[string] {
	elements := make([]string, 0, len(s.elements))
	for _, val := range s.elements {
		ref := reflect.ValueOf(val)
		if ref.Kind() == reflect.Pointer {
			elements = append(elements, fmt.Sprint(ref.Elem().Interface()))
		} else {
			elements = append(elements, fmt.Sprint(ref.Interface()))
		}
	}
	return &Series[string]{
		elements: elements,
		name:     s.name,
	}
}

func (s *Series[E]) Any() *Series[any] {
	elements := make([]any, 0, len(s.elements))
	for _, e := range s.elements {
		elements = append(elements, e)
	}
	return &Series[any]{
		name:     s.name,
		elements: elements,
	}
}

func (s *Series[E]) Get(i int) E {
	return s.elements[i]
}

func (s *Series[E]) Drop(value E) *Series[E] {
	elements := make([]E, 0, len(s.elements)/2)
	for _, val := range s.elements {
		if any(val) == any(value) {
			continue
		}
		elements = append(elements, val)
	}
	return &Series[E]{
		name:     s.name,
		elements: elements,
	}
}

func (s *Series[E]) DropDuplicates() *Series[E] {
	seen := make(map[any]struct{}, 0)
	elements := make([]E, 0)
	for _, val := range s.elements {
		v := any(val)
		if _, ok := seen[v]; ok {
			continue
		} else {
			seen[v] = struct{}{}
			elements = append(elements, val)
		}
	}

	return &Series[E]{
		name:     s.name,
		elements: elements,
	}
}

func (s *Series[E]) DropNan() *Series[E] {
	elements := make([]E, 0, len(s.elements)/2)
	for _, val := range s.elements {
		if v := any(val); v == nil {
			continue
		}
		switch v := any(val).(type) {
		case float32:
			if !math.IsNaN(float64(v)) {
				elements = append(elements, val)
			}
		case *float32:
			if !math.IsNaN(float64(*v)) {
				elements = append(elements, val)
			}
		case float64:
			if !math.IsNaN(v) {
				elements = append(elements, val)
			}
		case *float64:
			if !math.IsNaN(*v) {
				elements = append(elements, val)
			}
		default:
			elements = append(elements, val)
		}
	}
	return &Series[E]{
		name:     s.name,
		elements: elements,
	}
}

func (s *Series[E]) Print(limit int, info bool) string {
	indexes := make([]string, 0, limit)
	values := make([]string, 0, limit)
	valueLen := 0
	indexLen := 0
	length := len(s.elements)
	if length > limit {
		for i := 0; i < limit/2; i++ {
			value := fmt.Sprint(s.elements[i])
			if l := len(value); l > valueLen {
				valueLen = l
			}
			values = append(values, value)

			index := strconv.Itoa(i)
			if l := len(index); l > indexLen {
				indexLen = l
			}
			indexes = append(indexes, index)
		}
		for i := len(s.elements) - limit/2; i < len(s.elements); i++ {
			value := fmt.Sprint(s.elements[i])
			if l := len(value); l > valueLen {
				valueLen = l
			}
			values = append(values, value)

			index := strconv.Itoa(i)
			if l := len(index); l > indexLen {
				indexLen = l
			}
			indexes = append(indexes, index)
		}
	} else {
		for i, val := range s.elements {
			value := fmt.Sprint(val)
			if l := len(value); l > valueLen {
				valueLen = l
			}
			values = append(values, value)

			index := strconv.Itoa(i)
			if l := len(index); l > indexLen {
				indexLen = l
			}
			indexes = append(indexes, index)
		}
	}

	buf := new(bytes.Buffer)
	if valueLen < len("...") {
		valueLen = len("...")
	}
	for i := 0; i < limit/2 && i < len(values); i++ {
		buf.WriteString(fmt.Sprintf("%-"+strconv.Itoa(indexLen)+"s", indexes[i]))
		buf.WriteString("\t")
		buf.WriteString(fmt.Sprintf("%"+strconv.Itoa(valueLen)+"s", values[i]))
		buf.WriteString("\n")
	}
	if len(s.elements) > 10 {
		buf.WriteString(fmt.Sprintf("%-"+strconv.Itoa(indexLen)+"s", ""))
		buf.WriteString("\t")
		buf.WriteString(fmt.Sprintf("%"+strconv.Itoa(valueLen)+"s", "..."))
		buf.WriteString("\n")
	}
	for i := limit / 2; i < len(values); i++ {
		buf.WriteString(fmt.Sprintf("%-"+strconv.Itoa(indexLen)+"s", indexes[i]))
		buf.WriteString("\t")
		buf.WriteString(fmt.Sprintf("%"+strconv.Itoa(valueLen)+"s", values[i]))
		buf.WriteString("\n")
	}

	if info {
		buf.WriteString("Length: ")
		buf.WriteString(strconv.Itoa(len(s.elements)))
		buf.WriteString(", dtype: ")
		buf.WriteString(s.DType().String())
	}
	return buf.String()
}

func (s *Series[E]) String() string {
	return s.Print(10, true)
}

func NewSeries[E any](name string, vals ...E) *Series[E] {
	s := Series[E]{
		elements: vals,
		name:     name,
	}
	return &s
}
