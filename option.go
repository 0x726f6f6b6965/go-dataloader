package dataloader

import (
	"time"
)

const (
	DEFAULT_WAIT     time.Duration = 30 * time.Millisecond
	DEFAULT_MAXBATCH int           = 200
)

type Options[KeyT comparable, ValT any] struct {
	// the amount of time to wait before triggering a batch
	wait time.Duration

	// this will limit the maximum number of keys to send in one batch, 0 = no limit
	maxBatch int

	// the cache to use
	cache Cache[KeyT, ValT]

	// the tracer to use
	tracer Tracer[KeyT, ValT]
}

// Option is a function that can be used to set options on a Loader
type Option[KeyT comparable, ValT any] func(*Options[KeyT, ValT])

// WithWait sets the wait time for the loader
func WithWait[KeyT comparable, ValT any](wait time.Duration) Option[KeyT, ValT] {
	return func(o *Options[KeyT, ValT]) {
		o.wait = wait
	}
}

// WithMaxBatch sets the maximum number of keys to send in one batch
func WithMaxBatch[KeyT comparable, ValT any](maxBatch int) Option[KeyT, ValT] {
	return func(o *Options[KeyT, ValT]) {
		if maxBatch < 0 {
			maxBatch = DEFAULT_MAXBATCH
		}
		o.maxBatch = maxBatch
	}
}

// WithCache sets the cache for the loader
func WithCache[KeyT comparable, ValT any](cache Cache[KeyT, ValT]) Option[KeyT, ValT] {
	return func(o *Options[KeyT, ValT]) {
		o.cache = cache
	}
}

// WithTracer sets the tracer for the loader
func WithTracer[KeyT comparable, ValT any](tracer Tracer[KeyT, ValT]) Option[KeyT, ValT] {
	return func(o *Options[KeyT, ValT]) {
		o.tracer = tracer
	}
}
