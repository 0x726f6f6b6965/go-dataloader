package dataloader

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type LoaderInterface[KeyT comparable, ValT any] interface {
	// LoadOrExec loads or executes data depending on the ExecFunc function,
	// it returns a function that returns the result of the operation
	LoadOrExec(ctx context.Context, data Item[KeyT, ValT]) func() Result[ValT]
	// Clear clears a key from the cache
	Clear(key KeyT)
	// ClearAll clears all keys from the cache
	ClearAll()
}

var _ LoaderInterface[any, any] = &Loader[any, any]{}

// NewDataLoader creates a new instance of Loader
func NewDataLoader[KeyT comparable, ValT any](
	execFn ExecFunc[KeyT, ValT], option ...Option[KeyT, ValT]) *Loader[KeyT, ValT] {
	loader := &Loader[KeyT, ValT]{
		Options: &Options[KeyT, ValT]{
			wait:     DEFAULT_WAIT,
			maxBatch: DEFAULT_MAXBATCH,
			cache:    DefaultCache[KeyT, ValT]{},
			tracer:   DefaultTracer[KeyT, ValT]{},
		},
		do: execFn,
	}

	for _, opt := range option {
		opt(loader.Options)
	}
	return loader
}

func (l *Loader[KeyT, ValT]) LoadOrExec(ctx context.Context,
	key Item[KeyT, ValT]) func() Result[ValT] {
	if val, ok := l.cache.Get(key.Key); ok {
		return func() Result[ValT] {
			return Result[ValT]{Val: val, Err: nil}
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.batcher == nil {
		l.initBatcher(ctx)
		batcher := l.batcher
		go l.doBatch(batcher)
		go l.triggerBatch(batcher)
	}
	batcher := l.batcher
	// add item to batch
	l.diff++
	batcher.input <- key
	if l.diff >= l.maxBatch {
		// trigger batch
		batcher.isTrigger = true
		close(batcher.input)
		close(l.full)
		l.resetBatch(batcher)
	}
	result := func() Result[ValT] {
		<-batcher.done
		err, ok := batcher.errs[key.Key]
		if ok && err != nil {
			return Result[ValT]{Err: err}
		}

		val, ok := batcher.output[key.Key]
		if !ok {
			return Result[ValT]{Err: ErrKeyNotFound}
		} else {
			var panicErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicErr = fmt.Errorf("%v", r)
					}
				}()
				l.cache.Add(key.Key, val)
			}()
			if panicErr != nil {
				return Result[ValT]{
					Err: errors.Join(ErrPanicAddValInCache, panicErr),
				}
			}
			return Result[ValT]{Val: val}
		}
	}
	return result
}

func (l *Loader[KeyT, ValT]) Clear(key KeyT) {
	l.cache.Remove(key)
}

func (l *Loader[KeyT, ValT]) ClearAll() {
	l.cache.Purge()
}

func (l *Loader[KeyT, ValT]) initBatcher(ctx context.Context) {
	batch := &batchItem[KeyT, ValT]{
		ctx:    ctx,
		input:  make(chan Item[KeyT, ValT], l.maxBatch),
		output: make(map[KeyT]ValT),
		errs:   make(map[KeyT]error),
		done:   make(chan struct{}),
	}
	l.batcher = batch
	l.full = make(chan struct{})
}

func (l *Loader[KeyT, ValT]) doBatch(batcher *batchItem[KeyT, ValT]) {
	var (
		panicErr error
		items    = make([]Item[KeyT, ValT], 0, l.maxBatch)
	)
	for item := range batcher.input {
		items = append(items, item)
	}
	ctx, done := l.tracer.TraceBatch(batcher.ctx, items)
	defer done()
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = fmt.Errorf("%v", r)
			}
		}()
		batcher.output, batcher.errs = l.do(ctx, items)
	}()
	// panic
	if panicErr != nil {
		batcher.errs = make(map[KeyT]error)
		for _, item := range items {
			batcher.errs[item.Key] = errors.Join(ErrPanicBatch, panicErr)
		}
	}
	// batch done
	close(batcher.done)
}

func (l *Loader[KeyT, ValT]) triggerBatch(batcher *batchItem[KeyT, ValT]) {
	select {
	case <-l.full:
		return
	case <-time.After(l.wait):
		l.mu.Lock()
		if !batcher.isTrigger {
			// trigger batch
			batcher.isTrigger = true
			close(batcher.input)
			l.resetBatch(batcher)
		}
		l.mu.Unlock()
	}
}

func (l *Loader[KeyT, ValT]) resetBatch(batcher *batchItem[KeyT, ValT]) {
	if l.batcher == batcher {
		// reset batcher
		l.batcher = nil
		l.diff = 0
		l.full = nil
	}
}
