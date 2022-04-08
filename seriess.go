package pandat

type Seriess[E any] []*Series[E]

func (s Seriess[E]) Slice() [][]E {
	ret := make([][]E, 0, len(s))
	for _, values := range s {
		ret = append(ret, values.Slice())
	}
	return ret
}
