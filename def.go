package dataloader

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrPanicBatch         = errors.New("panic received in batch")
	ErrPanicAddValInCache = errors.New("panic received in add value in cache")
)

// Result is a struct that contains the result of a function.
type Result[ValT any] struct {
	Val ValT
	Err error
}

// Item is a struct that contains a key-value pair.
type Item[KeyT comparable, ValT any] struct {
	Key KeyT
	Val ValT
}

// ExecFunc is a function that
// receives a context and a slice of items,
// and returns a map of values and a map of errors.
type ExecFunc[KeyT comparable, ValT any] func(
	ctx context.Context, data []Item[KeyT, ValT]) (
	map[KeyT]ValT, map[KeyT]error)

// Loader is a struct that contains the options and the batcher.
type Loader[KeyT comparable, ValT any] struct {
	*Options[KeyT, ValT]

	mu sync.Mutex

	do ExecFunc[KeyT, ValT]

	batcher *batchItem[KeyT, ValT]

	diff int

	full chan struct{}
}

// batchItem is a struct that contains the context, the input channel,
// the output map, the errors map, the done channel, and a boolean to
// check if the batcher is a trigger.
type batchItem[KeyT comparable, ValT any] struct {
	ctx       context.Context
	input     chan Item[KeyT, ValT]
	output    map[KeyT]ValT
	errs      map[KeyT]error
	isTrigger bool
	done      chan struct{}
}
