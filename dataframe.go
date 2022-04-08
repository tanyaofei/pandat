package pandat

import (
	"fmt"
	"strconv"
	"strings"
)

type DataFrame[E any] struct {
	seriess []*Series[E]
	index   map[string]int
}

// Get return a series by giving name
func (d *DataFrame[E]) Get(name string) *Series[E] {
	if i, ok := d.index[name]; !ok {
		return nil
	} else {
		return d.seriess[i]
	}
}

func (d *DataFrame[E]) GetByIndex(i int) *Series[E] {
	return d.seriess[i]
}

// Val Return value by giving row and col
// row: index of row
// col: index of column of name of column, type: string | int
func (d *DataFrame[E]) Val(row int, col any) E {
	var series *Series[E]
	switch c := col.(type) {
	case int:
		series = d.seriess[c]
	case string:
		if c, ok := d.index[c]; ok {
			series = d.seriess[c]
		} else {
			panic("pandat.Dataframe.Val::index out of bound: " + fmt.Sprint(col))
		}
	}

	return series.Get(row)
}

// Location Return sub-DataFrame by giving rows and cols
// Location(":", 1)
// Location("1:5", "2:")
// Location(":5", "2:")
// Location([]int{1, 2, 3, 4, 5}, ":2")
// Location([]int{1, 2, 3, 4, 5}, nil)
func (d *DataFrame[E]) Location(rows any, cols any) *DataFrame[E] {
	var (
		irows = d.parseLocationExpr(rows)
		icols = d.parseLocationExpr(cols)
	)

	seriess := make([]*Series[E], 0, len(icols))
	for i, series := range d.seriess {
		if _, ok := icols[i]; ok {
			seriess = append(seriess, series)
		}
	}

	for i, series := range seriess {
		seriess[i] = series.SubSeriesByIndexer(irows)
	}

	df := &DataFrame[E]{
		seriess: seriess,
	}
	df.Reindex()
	return df
}

// Series Return the first series or nil if dataframe is empty
func (d *DataFrame[E]) Series() *Series[E] {
	if len(d.seriess) < 1 {
		return nil
	}
	return d.seriess[0]
}

// Seriess return series array in dataframe
func (d *DataFrame[E]) Seriess() []*Series[E] {
	return d.seriess
}

// DropColumn delete given column name or index
func (d *DataFrame[E]) DropColumn(indexOrName any, inplace bool) *DataFrame[E] {
	var index int
	switch ion := indexOrName.(type) {
	case string:
		index = d.Index(ion)
	case int:
		index = ion
	}

	seriess := append(d.seriess[:index], d.seriess[index+1:]...)
	if inplace {
		d.seriess = seriess
		d.Reindex()
		return d
	} else {
		df := &DataFrame[E]{
			seriess: seriess,
		}
		df.Reindex()
		return df
	}
}

// NCols return number of columns
func (d *DataFrame[E]) NCols() int {
	return len(d.seriess)
}

// NRows return number of rows
func (d *DataFrame[E]) NRows() int {
	if len(d.seriess) == 0 {
		return 0
	}

	return d.seriess[0].Len()
}

// Shape return nrows and ncols
func (d *DataFrame[E]) Shape() (int, int) {
	return d.NRows(), d.NCols()
}

// Names return seriess names
func (d *DataFrame[E]) Names() []string {
	names := make([]string, 0, len(d.seriess))
	for _, series := range d.seriess {
		names = append(names, series.name)
	}
	return names
}

// Name returns name of given index
func (d *DataFrame[E]) Name(index int) string {
	return d.seriess[index].name
}

// Index returns index of given name
func (d DataFrame[E]) Index(name string) int {
	return d.index[name]
}

// Rename seriess by giving map
// renamer: map[string]string or map[int]string, key is the index of series of the name of series, value is new name
func (d *DataFrame[E]) Rename(renamer map[any]string, inplace bool) *DataFrame[E] {

	if inplace {
		for i, series := range d.seriess {
			if name, ok := renamer[i]; ok {
				series.name = name
			} else if name, ok := renamer[series.name]; ok {
				series.name = name
			}
		}

		d.Reindex()
		return d
	}

	seriess := make([]*Series[E], 0, len(d.seriess))
	for i, series := range d.seriess {
		if name, ok := renamer[i]; ok {
			seriess = append(seriess, series.Rename(name))
		} else if name, ok := renamer[series.name]; ok {
			seriess = append(seriess, series.Rename(name))
		} else {
			seriess = append(seriess, series)
		}
	}
	df := &DataFrame[E]{
		seriess: seriess,
	}
	df.Reindex()
	return df
}

// Insert column into dataframe at specified index.
// i: column index
// series: column values
func (d *DataFrame[E]) Insert(i int, series *Series[E]) *DataFrame[E] {
	names := newSet(d.Names()...)
	if names.Contains(series.name) {
		panic("pandat.dataframe.insert::duplicate series name: " + series.name)
	}

	seriess := make([]*Series[E], 0, len(d.seriess)+1)
	seriess = append(d.seriess[:i], series)
	seriess = append(seriess, d.seriess[i:]...)

	df := &DataFrame[E]{
		seriess: seriess,
	}
	df.Reindex()
	return df
}

// Concat other dataframe
func (d *DataFrame[E]) Concat(other *DataFrame[E]) *DataFrame[E] {
	if d.NRows() != other.NRows() {
		panic("pandat.dataframe.concat: length not match")
	}

	names := newSet(d.Names()...)
	for _, name := range other.Names() {
		if names.Contains(name) {
			panic("pandat.dataframe.concat: duplicate series name: " + name)
		}
	}

	seriess := make([]*Series[E], 0, len(d.seriess)+len(other.seriess))
	seriess = append(seriess, d.seriess...)
	seriess = append(seriess, other.seriess...)
	df := &DataFrame[E]{seriess: seriess}
	df.Reindex()
	return df
}

// Transpose the dataframe
func (d *DataFrame[E]) Transpose() *DataFrame[E] {
	seriess := make([]*Series[E], 0, d.NRows())
	for row := 0; row < d.NRows(); row++ {
		values := make([]E, 0, d.NCols())
		for col := 0; col < d.NCols(); col++ {
			values = append(values, d.Val(row, col))
		}
		seriess = append(seriess, NewSeries(strconv.Itoa(row), values...))
	}
	df := &DataFrame[E]{
		seriess: seriess,
	}
	df.Reindex()
	return df
}

// Reindex this dataframe
func (d *DataFrame[E]) Reindex() {
	index := make(map[string]int, len(d.seriess))
	for i, series := range d.seriess {
		index[series.name] = i
	}
	d.index = index
	if len(d.index) != len(d.seriess) {
		panic("pandat.dataframe.Reindex::all series should have different names")
	}
}

func (d *DataFrame[E]) Float64() *DataFrame[float64] {
	seriess := make([]*Series[float64], 0, len(d.seriess))
	for _, series := range d.seriess {
		seriess = append(seriess, series.Float64())
	}
	df := &DataFrame[float64]{
		seriess: seriess,
		index:   d.index,
	}
	return df
}

func (d *DataFrame[E]) Int() *DataFrame[int] {
	seriess := make([]*Series[int], 0, len(d.seriess))
	for _, series := range d.seriess {
		seriess = append(seriess, series.Int())
	}
	df := &DataFrame[int]{
		seriess: seriess,
		index:   d.index,
	}
	return df
}

func (d *DataFrame[E]) Int64() *DataFrame[int64] {
	seriess := make([]*Series[int64], 0, len(d.seriess))
	for _, series := range d.seriess {
		seriess = append(seriess, series.Int64())
	}
	df := &DataFrame[int64]{
		seriess: seriess,
		index:   d.index,
	}
	return df
}

func (d *DataFrame[E]) Str() *DataFrame[string] {
	seriess := make([]*Series[string], 0, len(d.seriess))
	for _, series := range d.seriess {
		seriess = append(seriess, series.Str())
	}
	df := &DataFrame[string]{
		seriess: seriess,
		index:   d.index,
	}
	return df
}

func (d *DataFrame[E]) Any() *DataFrame[any] {
	seriess := make([]*Series[any], 0, len(d.seriess))
	for _, series := range d.seriess {
		seriess = append(seriess, series.Any())
	}
	df := &DataFrame[any]{
		seriess: seriess,
		index:   d.index,
	}
	return df
}

func (d *DataFrame[E]) parseLocationExpr(expr any) map[int]struct{} {
	ret := make(map[int]struct{}, 32)
	switch e := expr.(type) {
	case int:
		ret[e] = struct{}{}
	case []int:
		for _, i := range e {
			ret[i] = struct{}{}
		}
	case string:
		fromto := strings.Split(e, ":")
		if len(fromto) != 2 {
			panic("pandat.dataframe.Location::Error cols expr: " + e)
		}
		sfrom := fromto[0]
		if sfrom == "" {
			sfrom = "0"
		}
		sto := fromto[1]
		if sto == "" {
			sto = strconv.Itoa(len(d.seriess))
		}

		from, err := strconv.Atoi(sfrom)
		if err != nil {
			panic("pandat.dataframe.Location::Error cols expr: " + e)
		}
		to, err := strconv.Atoi(sto)
		if err != nil {
			panic("pandat.dataframe.Location::Error cols expr: " + e)
		}

		for i := from; i < to; i++ {
			ret[i] = struct{}{}
		}
	}

	return ret
}

func (d *DataFrame[E]) Copy() *DataFrame[E] {
	return &DataFrame[E]{
		seriess: d.seriess,
		index:   d.index,
	}
}

// NewDataFrame Create a dataframe by given seriess
func NewDataFrame[E any](values ...*Series[E]) *DataFrame[E] {
	seriess := make([]*Series[E], 0, len(values))
	index := make(map[string]int, len(values))
	length := 0
	for i, val := range values {
		if i == 0 {
			length = val.Len()
		} else if length != val.Len() {
			panic("pandat.dataframe.NewDataFrame::all seriess must be the same length")
		}
		name := val.Name()
		if _, ok := index[name]; ok {
			panic("pandat.dataframe.NewDataFrame::duplicate series name: " + name)
		}
		seriess = append(seriess, val)
		index[name] = i
	}

	return &DataFrame[E]{
		seriess,
		index,
	}
}
