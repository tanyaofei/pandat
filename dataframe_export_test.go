package pandat

import (
	"os"
	"testing"
)

func TestToParquetWithInterfaceDataFrame(t *testing.T) {
	df := ReadMap(map[string][]string{
		"A": {"1", "2", "3"},
	})

	out, err := os.Create("1.parquet")
	if err != nil {
		panic(err)
	}
	err = df.ToParquet(out)
	if err != nil {
		panic(err)
	}
}

func TestToParquet(t *testing.T) {
	df := NewDataFrame(
		NewSeries("a _=(1)", 1, 2, 3, 4, 5),
		NewSeries("b", 1, 2, 3, 4, 5),
	)

	out, err := os.Create("1.parquet")
	if err != nil {
		panic(err)
	}
	err = df.ToParquet(out)
	if err != nil {
		panic(err)
	}
}

func TestToXlsx(t *testing.T) {
	df := NewDataFrame(
		NewSeries("A", 1, 2, 3, 4, 5),
		NewSeries("B", 2, 3, 4, 5, 6),
	)

	f, err := os.Create("1.xlsx")
	if err != nil {
		panic(err)
	}

	err = df.ToXlsx(f, WriteXlsxOption{})
	if err != nil {
		panic(err)
	}
}
