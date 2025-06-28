package dataloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	return l.loadItem(ctx, key)
}

// loadItem handles loading an item when it's not found in the cache.
// It initializes the batcher if necessary, adds the item to the batch,
// and returns a function to retrieve the result.
func (l *Loader[KeyT, ValT]) loadItem(ctx context.Context, key Item[KeyT, ValT]) func() Result[ValT] {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.initBatcherOnce(ctx)
	currentBatcher := l.batcher

	// Add item to batch
	l.diff++
	currentBatcher.input <- key

	if l.diff >= l.maxBatch {
		l.triggerAndResetBatch(currentBatcher)
	}

	return l.createResultFunc(ctx, key, currentBatcher)
}

// initBatcherOnce ensures the batcher is initialized only once per batch cycle.
// It uses sync.Once, which requires l.mu to be held if l.once itself could be reset (which it is in resetBatch).
// The l.mu lock is held by loadItem when calling this.
func (l *Loader[KeyT, ValT]) initBatcherOnce(ctx context.Context) {
	l.once.Do(func() {
		// This block is guaranteed to run at most once until l.once is reset.
		// l.mu is held by the caller (loadItem) when l.once.Do is invoked.
		l.initBatcher(ctx)
		// Start batch processing and timeout goroutines for the newly initialized batcher.
		// These goroutines operate on the l.batcher instance created in l.initBatcher.
		go l.doBatch(l.batcher) // l.batcher is the new batcher instance
		go l.triggerBatch(l.batcher) // l.batcher is the new batcher instance
	})
}

// triggerAndResetBatch handles the logic for explicitly triggering a batch,
// typically when it's full or a timeout occurs.
// This function MUST be called with l.mu held.
func (l *Loader[KeyT, ValT]) triggerAndResetBatch(batcherToTrigger *batchItem[KeyT, ValT]) {
	// Prevent double-triggering, which could lead to closing already closed channels.
	if batcherToTrigger.isTrigger {
		return
	}
	batcherToTrigger.isTrigger = true

	// Close the input channel to signal doBatch that no more items are coming for this batch.
	close(batcherToTrigger.input)

	// Close the 'full' channel to signal the triggerBatch goroutine for this batch
	// that the batch was triggered by size, not timeout.
	// This check is important because l.full might be nil if resetBatch was called concurrently
	// by a different path (though current logic with l.mu should prevent this specific nil case here).
	if l.full != nil {
		close(l.full)
	}

	// Reset loader state to prepare for the next batch.
	l.resetBatch(batcherToTrigger)
}

// createResultFunc creates a function that, when called, waits for the batch processing
// to complete (or for the calling context to be cancelled) and then retrieves the result for the given key.
// It captures the `ctx` from the `LoadOrExec` call associated with this specific key.
func (l *Loader[KeyT, ValT]) createResultFunc(ctx context.Context, key Item[KeyT, ValT], batcherForResult *batchItem[KeyT, ValT]) func() Result[ValT] {
	return func() Result[ValT] {
		// Use a channel to signal completion of batcherForResult.Wait()
		// This allows selecting between batch completion and context cancellation.
		waitCh := make(chan struct{})
		go func() {
			defer close(waitCh)
			batcherForResult.Wait() // Wait for the specific batch this item was part of to complete
		}()

		select {
		case <-waitCh:
			// Batch processing completed. Proceed to retrieve the result.
			if err, ok := batcherForResult.errs[key.Key]; ok && err != nil {
				return Result[ValT]{Err: err}
			}

			val, ok := batcherForResult.output[key.Key]
			if !ok {
				// This case should ideally be covered by an error in batcherForResult.errs if l.do is well-behaved.
				// However, if l.do somehow doesn't set an error and also doesn't provide output, this is a fallback.
				return Result[ValT]{Err: ErrKeyNotFound}
			}

			// Attempt to add the successfully retrieved value to the cache.
			// Includes panic recovery in case the cache implementation has issues.
			var panicErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicErr = fmt.Errorf("panic during cache.Add: %v", r)
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

		case <-ctx.Done():
			// The context associated with this specific LoadOrExec call was cancelled.
			return Result[ValT]{Err: ctx.Err()}
		}
	}
}

func (l *Loader[KeyT, ValT]) Clear(key KeyT) {
	l.cache.Remove(key)
}

func (l *Loader[KeyT, ValT]) ClearAll() {
	l.cache.Purge()
}

// initBatcher initializes a new batch item.
// It sets up channels for input, output, and errors.
// It also increments the Add count on the batchItem's WaitGroup
// to signal that a new batch processing goroutine (doBatch) will be started.
func (l *Loader[KeyT, ValT]) initBatcher(ctx context.Context) {
	batch := &batchItem[KeyT, ValT]{
		ctx:    ctx,
		input:  make(chan Item[KeyT, ValT], l.maxBatch), // Buffered channel for batch items
		output: make(map[KeyT]ValT),
		errs:   make(map[KeyT]error),
	}
	batch.Add(1) // For the doBatch goroutine
	l.batcher = batch
	l.full = make(chan struct{}) // Channel to signal when a batch is full
}

// doBatch processes items from the batch's input channel.
// It collects items until the input channel is closed (signaling the batch is ready).
// Then, it executes the batch processing function (l.do) and stores the results.
// It recovers from panics during the execution of l.do.
// Finally, it calls Done on the batch's WaitGroup to signal completion.
func (l *Loader[KeyT, ValT]) doBatch(batcher *batchItem[KeyT, ValT]) {
	defer batcher.Done() // Signal completion when this function exits

	var (
		panicErr error
		items    = make([]Item[KeyT, ValT], 0, l.maxBatch)
	)

	// Collect all items from the input channel until it's closed
	for item := range batcher.input {
		items = append(items, item)
	}

	// If no items were received, there's nothing to process.
	if len(items) == 0 {
		return
	}

	ctx, done := l.tracer.TraceBatch(batcher.ctx, items)
	defer done() // Ensure tracing span is closed

	// Execute the batch processing function with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = fmt.Errorf("panic during batch execution: %v", r)
			}
		}()
		batcher.output, batcher.errs = l.do(ctx, items)
	}()

	if panicErr != nil {
		// If a panic occurred, create an error for each item in the batch.
		if batcher.errs == nil {
			batcher.errs = make(map[KeyT]error)
		}
		for _, item := range items {
			batcher.errs[item.Key] = errors.Join(ErrPanicBatch, panicErr)
		}
	}
}

// triggerBatch waits for either the batch to become full or a timeout to occur.
// If a timeout occurs and the batch hasn't been triggered yet, it triggers the batch.
// This function runs in a separate goroutine for each batch.
// The `batcher` argument is the specific batch instance this goroutine is responsible for.
func (l *Loader[KeyT, ValT]) triggerBatch(batcher *batchItem[KeyT, ValT]) {
	select {
	// Case 1: Batch was triggered by becoming full (l.full channel is closed).
	// l.full is associated with the loader's *current* batch.
	// If l.full closes, it means the batch that this triggerBatch was *intended* for,
	// or a subsequent one, got filled and triggered.
	case <-l.full:
		return
	case <-time.After(l.wait): // Timeout occurred
		l.mu.Lock()
		defer l.mu.Unlock()
		// Check if this specific batcher still needs to be triggered.
		// It's possible a new batcher was created if the previous one filled up.
		if l.batcher == batcher && !batcher.isTrigger {
			l.triggerAndResetBatch(batcher)
		}
	}
}

// resetBatch resets the loader's batch-related fields to prepare for a new batch.
// This is called when a batch is triggered either by becoming full or by timeout.
// It ensures that the once variable is reset so that initBatcherOnce can correctly
// initialize the next batch.
// This function MUST be called with l.mu held.
func (l *Loader[KeyT, ValT]) resetBatch(triggeredBatcher *batchItem[KeyT, ValT]) {
	// Critical check: Only reset the loader's state if the batch being reset
	// is the *current* active batcher associated with the loader.
	// This prevents a delayed trigger (e.g., timeout from an old, already processed batch)
	// from incorrectly resetting the state of a newer, active batch.
	if l.batcher == triggeredBatcher {
		l.batcher = nil      // Clear the current batcher
		l.once = sync.Once{} // Reset sync.Once to allow the next batch to be initialized
		l.diff = 0           // Reset the count of items in the current (now cleared) batch
		l.full = nil         // Set l.full to nil; it was closed in triggerAndResetBatch
	}
	// If l.batcher != triggeredBatcher, it means the loader has already moved on to a new
	// batch (e.g., triggeredBatcher was filled and reset, and a new one was created).
	// In this scenario, we should not modify the loader's state, as it pertains to the new batch.
	// The triggeredBatcher (the old one) has already had its input channel closed
	// and its isTrigger flag set by triggerAndResetBatch.
}
