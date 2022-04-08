package pandat

import (
	"gonum.org/v1/gonum/stat"
	"math"
	"sort"
)

func (s *Series[E]) Quantile(p float64) float64 {
	if s.Len() == 0 {
		return math.NaN()
	}

	data := s.Float64().elements
	sort.Float64s(data)
	return stat.Quantile(p, stat.Empirical, data, nil)
}

func (s *Series[E]) Mean() float64 {
	return stat.Mean(s.Float64().elements, nil)
}

func (s *Series[E]) Sum() float64 {
	if s.Len() == 0 {
		return 0
	}

	sum := 0.0
	for _, val := range s.Float64().elements {
		sum += val
	}
	return sum
}

func (s *Series[E]) Median() float64 {
	if s.Len() == 0 {
		return math.NaN()
	}

	data := s.Float64().elements
	sort.Float64s(data)

	if len(data)%2 != 0 {
		return data[len(data)/2]
	}

	return (data[len(data)/2-1] + data[len(data)/2]) * 0.5
}

func (s *Series[E]) Mode() []E {
	seen := make(map[any]int, s.Len())

	max := 0
	for _, val := range s.elements {
		var count int
		if _, ok := seen[val]; ok {
			count = seen[val] + 1
		} else {
			count = 1
		}
		seen[val] = count
		if count > max {
			max = count
		}
	}

	modes := make([]E, 0)
	for val, count := range seen {
		if count == max {
			switch v := val.(type) {
			case E:
				modes = append(modes, v)
			default:
				panic("impossible")
			}
		}
	}

	return modes
}
