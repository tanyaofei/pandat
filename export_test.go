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
	err = df.toParquet(out)
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
	err = df.toParquet(out)
	if err != nil {
		panic(err)
	}
}
