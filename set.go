package pandat

type set[E any] struct {
	value map[any]struct{}
}

func (s *set[E]) Contains(val any) bool {
	_, ok := s.value[val]
	return ok
}

func (s *set[E]) ContainsAny(vals ...any) bool {
	for _, val := range vals {
		if _, ok := s.value[val]; ok {
			return true
		}
	}
	return false
}

func (s *set[E]) Add(val E) {
	s.value[any(val)] = struct{}{}
}

func (s *set[E]) Remove(val E) {
	delete(s.value, any(val))
}

func (s *set[E]) Size() int {
	return len(s.value)
}

func newSet[E any](vals ...E) *set[E] {
	values := make(map[any]struct{})
	for _, val := range vals {
		values[any(val)] = struct{}{}
	}
	return &set[E]{
		value: values,
	}
}
