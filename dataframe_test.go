package pandat

import (
	"fmt"
	"testing"
)

func TestDropColumn(t *testing.T) {
	df := NewDataFrame(
		NewSeries("a", 1, 2, 3, 4, 5),
	)

	df = df.DropColumn(0, true)
	fmt.Println(df)
}
