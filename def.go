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

type Result[ValT any] struct {
	Val ValT
	Err error
}

type Item[KeyT comparable, ValT any] struct {
	Key KeyT
	Val ValT
}

type ExecFunc[KeyT comparable, ValT any] func(
	ctx context.Context, data []Item[KeyT, ValT]) (
	map[KeyT]ValT, map[KeyT]error)

type Loader[KeyT comparable, ValT any] struct {
	*Options[KeyT, ValT]

	mu sync.Mutex

	do ExecFunc[KeyT, ValT]

	batcher *batchItem[KeyT, ValT]

	diff int

	full chan struct{}
}

type batchItem[KeyT comparable, ValT any] struct {
	ctx       context.Context
	input     chan Item[KeyT, ValT]
	output    map[KeyT]ValT
	errs      map[KeyT]error
	isTrigger bool
	done      chan struct{}
}
