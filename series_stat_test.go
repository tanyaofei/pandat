package pandat

import (
	"fmt"
	"testing"
)

func TestQuantile(t *testing.T) {
	series := NewSeries("test", 1, 2, 3)
	fmt.Println(series.Mean())
}

func TestMode(t *testing.T) {
	series := NewSeries("test", 1, 2, 3, 3, 4, 4, 3, 4, 41, 5, 6)
	fmt.Println(series.Mode())
}

func TestSum(t *testing.T) {
	series := NewSeries("test", 1, 2, 3, 4, 5)
	fmt.Println(series.Sum())
}
