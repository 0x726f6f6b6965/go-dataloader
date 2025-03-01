package dataloader

import "context"

type Tracer[KeyT comparable, ValT any] interface {
	// TraceBatch is called before a batch is executed
	TraceBatch(ctx context.Context, keys []Item[KeyT, ValT]) (context.Context, func())
}

// DefaultTracer is a tracer that does nothing
type DefaultTracer[KeyT comparable, ValT any] struct{}

func (DefaultTracer[KeyT, ValT]) TraceBatch(
	ctx context.Context, keys []Item[KeyT, ValT]) (
	context.Context, func()) {
	return ctx, func() {}
}
