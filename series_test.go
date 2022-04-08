package pandat

import (
	"fmt"
	"testing"
)

func TestNewSeries(*testing.T) {
	NewSeries("a", 1, 2, 3, 4, 5)
}

func TestReplace(*testing.T) {
	df := NewDataFrame(
		NewSeries("A", 1, 2, 3, 4, 5),
		NewSeries("B", 2, 3, 4, 5, 6),
	)
	df = df.Rename(map[interface{}]string{
		"A": "AA",
		"B": "BB",
	}, false).Transpose().Transpose().Transpose().Transpose()

	fmt.Println(df)

}
