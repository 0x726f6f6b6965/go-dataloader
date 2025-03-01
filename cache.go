package dataloader

type Cache[KeyT comparable, ValT any] interface {
	// Get returns the value stored in the cache and a boolean indicating if the value was found
	Get(key KeyT) (value ValT, ok bool)
	// Add adds a value to the cache and returns a boolean indicating if the value was evicted
	Add(key KeyT, value ValT) (evicted bool)
	// Remove removes a value from the cache and returns a boolean indicating if the value was found
	Remove(key KeyT) bool
	// Purge removes all values from the cache
	Purge()
}

// DefaultCache is a cache that does not store any values
type DefaultCache[KeyT comparable, ValT any] struct{}

func (DefaultCache[KeyT, ValT]) Get(_ KeyT) (ValT, bool) {
	var zero ValT
	return zero, false
}

func (DefaultCache[KeyT, ValT]) Add(_ KeyT, _ ValT) bool {
	return false
}

func (DefaultCache[KeyT, ValT]) Remove(_ KeyT) bool {
	return false
}

func (DefaultCache[KeyT, ValT]) Purge() {}
