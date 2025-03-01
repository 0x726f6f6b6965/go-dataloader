package mockopt

import "github.com/stretchr/testify/mock"

type MockPanicCache[KeyT comparable, ValT any] struct {
	mock.Mock
}

func (m *MockPanicCache[KeyT, ValT]) Get(key KeyT) (ValT, bool) {
	args := m.Called(key)
	return args.Get(0).(ValT), args.Bool(1)
}

func (*MockPanicCache[KeyT, ValT]) Add(_ KeyT, _ ValT) bool {
	panic("panic add")
}

func (m *MockPanicCache[KeyT, ValT]) Remove(key KeyT) bool {
	args := m.Called(key)
	return args.Bool(0)
}

func (*MockPanicCache[KeyT, ValT]) Purge() {
}
