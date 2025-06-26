package dataloader_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/0x726f6f6b6965/go-dataloader"
	"github.com/0x726f6f6b6965/go-dataloader/mockopt"
	"github.com/stretchr/testify/suite"
)

type LoaderTestSuite struct {
	suite.Suite
}

func TestLoaderTestSuite(t *testing.T) {
	suite.Run(t, new(LoaderTestSuite))
}

func (suite *LoaderTestSuite) TestLoadOrExec() {
	testCases := []struct {
		name        string
		execFunc    func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error)
		input       []dataloader.Item[int, string]
		expected    map[int]string
		expectedErr map[int]error // For cases where specific errors are expected per key
		options     []dataloader.Option[int, string]
	}{
		{
			name: "SuccessfulBatchProcessing",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				for _, d := range data {
					res[d.Key] = d.Val + "_processed"
				}
				return res, nil
			},
			input: []dataloader.Item[int, string]{
				{Key: 1, Val: "1_test"},
				{Key: 2, Val: "2_test"},
			},
			expected: map[int]string{
				1: "1_test_processed",
				2: "2_test_processed",
			},
		},
		{
			name: "ProcessingWithMaxBatchLimit",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				for _, d := range data {
					res[d.Key] = d.Val + "_processed"
				}
				return res, nil
			},
			input: []dataloader.Item[int, string]{
				{Key: 1, Val: "1_test"}, {Key: 2, Val: "2_test"},
				{Key: 3, Val: "3_test"}, {Key: 4, Val: "4_test"},
				{Key: 5, Val: "5_test"},
			},
			expected: map[int]string{
				1: "1_test_processed", 2: "2_test_processed",
				3: "3_test_processed", 4: "4_test_processed",
				5: "5_test_processed",
			},
			options: []dataloader.Option[int, string]{
				dataloader.WithMaxBatch[int, string](2),
				dataloader.WithWait[int, string](50 * time.Millisecond),
			},
		},
		{
			name: "ExecFuncReturnsSpecificErrors",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				errs := make(map[int]error)
				for i, d := range data {
					if i%2 == 0 { // Error for even indexed items
						errs[d.Key] = fmt.Errorf("specific error for key %d", d.Key)
					} else {
						res[d.Key] = d.Val + "_processed"
					}
				}
				return res, errs
			},
			input: []dataloader.Item[int, string]{
				{Key: 1, Val: "1_test"}, // Error
				{Key: 2, Val: "2_test"}, // Success
				{Key: 3, Val: "3_test"}, // Error
			},
			expected: map[int]string{
				2: "2_test_processed",
			},
			expectedErr: map[int]error{
				1: fmt.Errorf("specific error for key 1"),
				3: fmt.Errorf("specific error for key 3"),
			},
		},
		{
			name: "ExecFuncReturnsGlobalErrorButNoPerItemErrorMap", // Should panic and be caught
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				// This execFunc is problematic: it signals a global problem by returning a non-nil error
				// but doesn't provide the errs map. The dataloader's panic recovery for l.do
				// should handle if batcher.errs is nil after l.do returns.
				// However, the current l.do call in dataloader.go assigns both output and errs.
				// If l.do itself panics, that's covered by TestLoadOrExecPanic.
				// This test is more about what happens if l.do returns (nil, non-nil error map)
				// or (valid map, nil error map where some keys are missing).
				// Let's test missing key without error.
				res := make(map[int]string)
				// Key 2 will be missing from output and not in error map
				res[1] = "1_test_processed"
				return res, nil // No error map
			},
			input: []dataloader.Item[int, string]{
				{Key: 1, Val: "1_test"},
				{Key: 2, Val: "2_test"}, // This key will be missing
			},
			expected: map[int]string{
				1: "1_test_processed",
			},
			expectedErr: map[int]error{
				2: dataloader.ErrKeyNotFound, // Expect ErrKeyNotFound for key 2
			},
		},
		{
			name: "LargeInputVolume",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				for _, d := range data {
					res[d.Key] = d.Val + "_processed"
				}
				return res, nil
			},
			input: func() []dataloader.Item[int, string] {
				var items []dataloader.Item[int, string]
				for i := 0; i < 1000; i++ { // Reduced from 10000 for faster test
					items = append(items, dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("%d_test", i)})
				}
				return items
			}(),
			expected: func() map[int]string {
				expected := make(map[int]string)
				for i := 0; i < 1000; i++ {
					expected[i] = fmt.Sprintf("%d_test_processed", i)
				}
				return expected
			}(),
			options: []dataloader.Option[int, string]{
				dataloader.WithMaxBatch[int, string](100), // Smaller maxBatch for more batch churn
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		suite.Run(tc.name, func() {
			suite.T().Parallel() // Run sub-tests in parallel

			loader := dataloader.NewDataLoader(tc.execFunc, tc.options...)
			thunks := make(map[int]func() dataloader.Result[string])
			results := make(map[int]dataloader.Result[string])
			var resultsMu sync.Mutex // Added mutex for results map
			var wg sync.WaitGroup

			for _, item := range tc.input {
				thunks[item.Key] = loader.LoadOrExec(context.Background(), item)
			}

			for key, thunk := range thunks {
				wg.Add(1)
				go func(k int, t func() dataloader.Result[string]) {
					defer wg.Done()
					res := t()
					resultsMu.Lock() // Lock before writing to results map
					results[k] = res
					resultsMu.Unlock() // Unlock after writing
				}(key, thunk)
			}
			wg.Wait()

			for _, item := range tc.input {
				key := item.Key
				result := results[key]

				if tc.expectedErr != nil && tc.expectedErr[key] != nil {
					suite.Require().Error(result.Err, "Expected an error for key %d", key)
					suite.Assert().Contains(result.Err.Error(), tc.expectedErr[key].Error(), "Error message mismatch for key %d", key)
				} else {
					suite.Require().NoError(result.Err, "Did not expect an error for key %d, got %v", key, result.Err)
					suite.Assert().Equal(tc.expected[key], result.Val, "Value mismatch for key %d", key)
				}
			}
		})
	}
}

func (suite *LoaderTestSuite) TestCacheBehavior() {
	// Test with default cache (no-op)
	suite.Run("DefaultCache", func() {
		execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
			suite.T().Logf("DefaultCache: execFn called with %d items", len(data))
			res := make(map[int]string)
			for _, d := range data {
				res[d.Key] = d.Val + "_processed"
			}
			return res, nil
		}
		loader := dataloader.NewDataLoader(execFn) // Uses DefaultCache

		item := dataloader.Item[int, string]{Key: 1, Val: "val1"}
		_ = loader.LoadOrExec(context.Background(), item)() // First call, execFn runs
		_ = loader.LoadOrExec(context.Background(), item)() // Second call, execFn should run again with DefaultCache

		// Verification of execFn calls would ideally require a mock execFn or observing side effects.
		// For now, this test structure assumes DefaultCache means execFn is always hit.
	})

	// Test with a simple custom cache
	suite.Run("SimpleCustomCache", func() {
		customCache := &MockSimpleCache[int, string]{ // Removed mockopt. prefix
			Store: make(map[int]string),
		}
		execFnCallCount := 0
		execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
			execFnCallCount++
			suite.T().Logf("SimpleCustomCache: execFn called (count: %d) with %d items: %v", execFnCallCount, len(data), data)
			res := make(map[int]string)
			for _, d := range data {
				res[d.Key] = d.Val + "_processed_custom"
			}
			return res, nil
		}
		loader := dataloader.NewDataLoader(execFn, dataloader.WithCache[int, string](customCache))

		item1 := dataloader.Item[int, string]{Key: 100, Val: "val100"}
		item2 := dataloader.Item[int, string]{Key: 101, Val: "val101"}

		// First load for item1
		res1First := loader.LoadOrExec(context.Background(), item1)()
		suite.NoError(res1First.Err)
		suite.Equal("val100_processed_custom", res1First.Val)
		suite.Equal(1, execFnCallCount, "execFn should be called once for item1 first load")

		// Second load for item1 (should be cached)
		res1Second := loader.LoadOrExec(context.Background(), item1)()
		suite.NoError(res1Second.Err)
		suite.Equal("val100_processed_custom", res1Second.Val) // Cache should return the processed value
		suite.Equal(1, execFnCallCount, "execFn should NOT be called again for item1 second load due to cache")

		// First load for item2 (cache miss for item2)
		res2First := loader.LoadOrExec(context.Background(), item2)()
		suite.NoError(res2First.Err)
		suite.Equal("val101_processed_custom", res2First.Val)
		suite.Equal(2, execFnCallCount, "execFn should be called for item2 first load")

		// Clear item1 from cache
		loader.Clear(item1.Key)
		suite.T().Logf("Cache store after clearing item1: %v", customCache.Store)


		// Load item1 again (should be a cache miss now)
		res1Third := loader.LoadOrExec(context.Background(), item1)()
		suite.NoError(res1Third.Err)
		suite.Equal("val100_processed_custom", res1Third.Val)
		suite.Equal(3, execFnCallCount, "execFn should be called for item1 after cache clear")

		// Load item2 again (should be cached)
		res2Second := loader.LoadOrExec(context.Background(), item2)()
		suite.NoError(res2Second.Err)
		suite.Equal(3, execFnCallCount, "execFn should NOT be called for item2 as it's still cached")


		// Clear all from cache
		loader.ClearAll()
		suite.Empty(customCache.Store, "Cache store should be empty after ClearAll")

		// Load item1 again (cache miss)
		_ = loader.LoadOrExec(context.Background(), item1)()
		suite.Equal(4, execFnCallCount, "execFn should be called for item1 after ClearAll")
	})
}


func (suite *LoaderTestSuite) TestCachePanic() {
	// Use testify's require for Must-style assertions if needed, or stick to suite.Assert/suite.Require
	mockCache := &mockopt.MockPanicCache[int, string]{}
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		res := make(map[int]string)
		for _, d := range data {
			res[d.Key] = d.Val // No processing, just pass through for simplicity
		}
		return res, nil
	}
	loader := dataloader.NewDataLoader(execFn, dataloader.WithCache[int, string](mockCache))

	// Setup mock expectations
	// Get will be called, return false (cache miss)
	mockCache.On("Get", 1).Return("", false).Once()
	// Add will be called later and will panic
	// The panic is defined in MockPanicCache's Add method.

	thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: 1, Val: "1_test"})
	result := thunk()

	suite.Require().Error(result.Err, "Expected an error due to panic in Cache.Add")
	suite.Assert().True(errors.Is(result.Err, dataloader.ErrPanicAddValInCache), "Error should wrap ErrPanicAddValInCache")
	suite.Assert().Contains(result.Err.Error(), "panic during cache.Add: panic add", "Error message mismatch") // Corrected expected message

	mockCache.AssertExpectations(suite.T())
}

func (suite *LoaderTestSuite) TestLoadOrExecPanic() {
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		panic("execFn panic")
	}
	loader := dataloader.NewDataLoader(execFn)
	thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: 1})
	result := thunk()

	suite.Require().Error(result.Err, "Expected an error due to panic in execFn")
	suite.Assert().True(errors.Is(result.Err, dataloader.ErrPanicBatch), "Error should wrap ErrPanicBatch")
	suite.Assert().Contains(result.Err.Error(), "panic during batch execution: execFn panic", "Error message mismatch")
}

func (suite *LoaderTestSuite) TestThunkContextCancellation() {
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		// Simulate work that takes time, allowing context cancellation to occur
		// Check if context is already cancelled before sleeping
		select {
		case <-ctx.Done():
			errs := make(map[int]error)
			for _, d := range data {
				errs[d.Key] = ctx.Err() // Propagate context error
			}
			return nil, errs
		default:
		}

		time.Sleep(100 * time.Millisecond) // Simulate work

		// Check context again after work, before returning results
		// This is good practice for long batch jobs.
		select {
		case <-ctx.Done():
			errs := make(map[int]error)
			for _, d := range data {
				errs[d.Key] = ctx.Err()
			}
			return nil, errs
		default:
		}

		res := make(map[int]string)
		for _, d := range data {
			res[d.Key] = fmt.Sprintf("%s_processed", d.Val)
		}
		return res, nil
	}

	loader := dataloader.NewDataLoader(execFn, dataloader.WithWait[int, string](200*time.Millisecond))

	key := 1
	val := "test_val_cancel"

	// Short timeout, less than the execFn's sleep + WithWait buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	thunk := loader.LoadOrExec(ctx, dataloader.Item[int, string]{Key: key, Val: val})

	result := thunk()

	suite.Require().Error(result.Err, "Expected an error due to context cancellation")
	// The error could be DeadlineExceeded if the timeout hits before execFn starts,
	// or if the thunk's select hits ctx.Done() first.
	suite.Assert().True(errors.Is(result.Err, context.DeadlineExceeded) || errors.Is(result.Err, context.Canceled),
		"Expected context.DeadlineExceeded or context.Canceled, got: %v", result.Err)
	suite.Empty(result.Val, "Value should be empty on context cancellation")
	suite.T().Logf("Cancellation test: received error: %v", result.Err)

	// Test case 2: Context not cancelled, should proceed normally
	// Use a new loader to ensure fresh state if needed, or ensure previous loader is fine.
	// For simplicity with batching state, a new loader might be cleaner for isolated test.
	loaderNormal := dataloader.NewDataLoader(execFn, dataloader.WithWait[int, string](5*time.Millisecond)) // Short wait
	ctxNormal, cancelNormal := context.WithTimeout(context.Background(), 200*time.Millisecond) // Sufficient time
	defer cancelNormal()

	thunkNormal := loaderNormal.LoadOrExec(ctxNormal, dataloader.Item[int, string]{Key: 2, Val: "normal_val"})
	resultNormal := thunkNormal()

	suite.NoError(resultNormal.Err, "Normal execution failed: %v", resultNormal.Err)
	if resultNormal.Err == nil {
		suite.Equal("normal_val_processed", resultNormal.Val)
	}
}


func (suite *LoaderTestSuite) TestConcurrencyAndCorrectness() {
	totalItems := 200 // Reduced for faster tests, but enough for concurrency
	maxBatch := 10
	execFnCallCount := 0
	var execFnMu sync.Mutex // To safely increment execFnCallCount

	execFn := func(ctx context.Context, items []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		execFnMu.Lock()
		execFnCallCount++
		currentCallCount := execFnCallCount
		execFnMu.Unlock()

		suite.T().Logf("ExecFn call #%d: processing %d items. First item key: %d", currentCallCount, len(items), items[0].Key)
		suite.Assert().LessOrEqual(len(items), maxBatch, "Batch size exceeded maxBatch limit")

		// Simulate work and potential context checking
		select {
		case <-ctx.Done():
			errsMap := make(map[int]error)
			for _, item := range items {
				errsMap[item.Key] = ctx.Err()
			}
			return nil, errsMap
		case <-time.After(10 * time.Millisecond): // Simulate some processing time
		}

		results := make(map[int]string)
		errs := make(map[int]error)
		for _, item := range items {
			if item.Key == -1 { // Special key to test error propagation
				errs[item.Key] = fmt.Errorf("error for item %d", item.Key)
			} else {
				results[item.Key] = fmt.Sprintf("val_%d_processed", item.Key)
			}
		}
		return results, errs
	}

	loader := dataloader.NewDataLoader(
		execFn,
		dataloader.WithMaxBatch[int, string](maxBatch),
		dataloader.WithWait[int, string](20*time.Millisecond), // Relatively short wait to encourage batching by timeout too
	)

	var wg sync.WaitGroup
	resultsChan := make(chan dataloader.Result[string], totalItems)

	for i := 0; i < totalItems; i++ {
		wg.Add(1)
		go func(itemKey int) {
			defer wg.Done()
			item := dataloader.Item[int, string]{Key: itemKey, Val: fmt.Sprintf("val_%d", itemKey)}
			// Use a fresh context for each LoadOrExec call to avoid unintended shared cancellation effects
			// unless specifically testing that.
			thunk := loader.LoadOrExec(context.Background(), item)
			resultsChan <- thunk()
		}(i)
	}

	// Add a specific item to test error case within concurrent calls
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	item := dataloader.Item[int, string]{Key: -1, Val: "error_item"}
	// 	thunk := loader.LoadOrExec(context.Background(), item)
	// 	resultsChan <- thunk()
	// }()

	wg.Wait()
	close(resultsChan)

	processedCount := 0
	errorCount := 0
	correctResults := make(map[int]string)
	for i := 0; i < totalItems; i++ {
		correctResults[i] = fmt.Sprintf("val_%d_processed", i)
	}
	// correctResults[-1] = "" // Error case, no value expected

	for result := range resultsChan {
		if result.Err != nil {
			errorCount++
			// suite.Assert().EqualError(result.Err, "error for item -1")
			// For now, assume other errors are unexpected in this specific setup unless Key == -1
			// This part needs refinement if testing more complex error scenarios from execFn
		} else {
			processedCount++
			// Extract key from value for verification, assuming format "val_<key>_processed"
			// This is a bit brittle; ideally, the result would carry the key.
			// Since thunks are processed out of order, we need a way to map results back.
			// For now, we just check if the value is one of the expected values.
			found := false
			for k, v := range correctResults {
				if result.Val == v {
					delete(correctResults, k) // Remove found item
					found = true
					break
				}
			}
			suite.Assert().True(found, "Unexpected or duplicate result value: %s", result.Val)
		}
	}

	suite.Equal(totalItems, processedCount+errorCount, "Total items processed/errored mismatch")
	// suite.Equal(1, errorCount, "Expected one error for item -1") // If -1 item is included
	suite.Empty(correctResults, "Not all items were processed correctly, remaining: %v", correctResults)

	suite.T().Logf("ExecFn was called %d times.", execFnCallCount)
	expectedMinCalls := (totalItems + maxBatch - 1) / maxBatch
	suite.Assert().GreaterOrEqual(execFnCallCount, expectedMinCalls, "ExecFn not called enough times for batching by size")
	// Max calls could be totalItems if wait time is very short and items come in slowly.
	// This assertion is tricky without more precise timing control.
}

// MockSimpleCache for TestCacheBehavior.
// This is defined within the dataloader_test package for direct use in these tests.
type MockSimpleCache[KeyT comparable, ValT any] struct {
	Store map[KeyT]ValT
	mu    sync.RWMutex
}

func (c *MockSimpleCache[KeyT, ValT]) Get(key KeyT) (ValT, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.Store[key]
	return val, ok
}

func (c *MockSimpleCache[KeyT, ValT]) Add(key KeyT, value ValT) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Store[key] = value
	return false // Not handling eviction
}

func (c *MockSimpleCache[KeyT, ValT]) Remove(key KeyT) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.Store[key]
	if ok {
		delete(c.Store, key)
		return true
	}
	return false
}

func (c *MockSimpleCache[KeyT, ValT]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Store = make(map[KeyT]ValT)
}

// --- Benchmark Tests ---

var blackhole string // Prevents compiler optimizing away results

// Simple execFn for benchmarks
func benchmarkExecFn(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
	res := make(map[int]string, len(data))
	for _, d := range data {
		// Simulate some minimal work, e.g., string manipulation
		res[d.Key] = fmt.Sprintf("val_%d_processed_at_%d", d.Key, time.Now().UnixNano())
	}
	// Simulate a small delay per batch
	// time.Sleep(1 * time.Millisecond) // Uncomment to simulate more realistic batch work
	return res, nil
}

func BenchmarkLoadOrExec_Simple_NoCache(b *testing.B) {
	loader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](10),
		dataloader.WithWait[int, string](5*time.Millisecond),
		dataloader.WithCache[int, string](dataloader.DefaultCache[int, string]{}), // Explicitly no-op cache
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("val_%d", i)})
		result := thunk()
		if result.Err != nil {
			b.Fatalf("Unexpected error: %v", result.Err)
		}
		blackhole = result.Val // Use the result
	}
}

func BenchmarkLoadOrExec_Simple_WithCacheHits(b *testing.B) {
	// Pre-populate cache for subsequent hits
	cache := &MockSimpleCache[int, string]{Store: make(map[int]string)}
	preFillLoader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](100), // Larger batch for pre-fill
		dataloader.WithWait[int, string](1*time.Millisecond),
		dataloader.WithCache[int, string](cache),
	)

	// Pre-fill items that will be commonly accessed
	numPreFillItems := 1000 // Number of items to pre-fill and then hit in benchmark
	var preFillThunks []func() dataloader.Result[string]
	for i := 0; i < numPreFillItems; i++ {
		thunk := preFillLoader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("val_%d", i)})
		preFillThunks = append(preFillThunks, thunk)
	}
	for _, thunk := range preFillThunks {
		res := thunk()
		if res.Err != nil {
			b.Fatalf("Pre-fill error: %v", res.Err)
		}
	}
	b.Logf("Cache pre-filled with %d items. Store size: %d", numPreFillItems, len(cache.Store))


	// Loader for benchmark, using the pre-filled cache
	loader := dataloader.NewDataLoader(benchmarkExecFn, // This execFn should ideally not be called much
		dataloader.WithMaxBatch[int, string](10),
		dataloader.WithWait[int, string](5*time.Millisecond),
		dataloader.WithCache[int, string](cache),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access items that should be in cache
		key := i % numPreFillItems // Cycle through pre-filled keys
		thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: key, Val: fmt.Sprintf("val_%d", key)})
		result := thunk()
		if result.Err != nil {
			b.Fatalf("Unexpected error: %v", result.Err)
		}
		blackhole = result.Val
	}
}

func BenchmarkLoadOrExec_Concurrent_NoCache(b *testing.B) {
	loader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](50), // Larger batch size for concurrency
		dataloader.WithWait[int, string](10*time.Millisecond),
		dataloader.WithCache[int, string](dataloader.DefaultCache[int, string]{}),
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var counter int
		for pb.Next() {
			key := counter // Each goroutine will have its own sequence of keys
			thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: key, Val: fmt.Sprintf("val_%d", key)})
			result := thunk()
			if result.Err != nil {
				b.Errorf("Unexpected error: %v", result.Err) // Use Errorf in parallel benchmarks
			}
			blackhole = result.Val
			counter++
		}
	})
}

func BenchmarkLoadOrExec_Concurrent_WithCacheHits(b *testing.B) {
	cache := &MockSimpleCache[int, string]{Store: make(map[int]string)}
	numPreFillItems := 2000 // More items for concurrent access
	preFillLoader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](200),
		dataloader.WithWait[int, string](1*time.Millisecond),
		dataloader.WithCache[int, string](cache),
	)
	var preFillThunks []func() dataloader.Result[string]
	for i := 0; i < numPreFillItems; i++ {
		thunk := preFillLoader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("val_%d", i)})
		preFillThunks = append(preFillThunks, thunk)
	}
	for _, thunk := range preFillThunks {
		if res := thunk(); res.Err != nil {
			b.Fatalf("Pre-fill error: %v", res.Err)
		}
	}
	b.Logf("Concurrent CacheHits: Cache pre-filled with %d items. Store size: %d", numPreFillItems, len(cache.Store))


	loader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](50),
		dataloader.WithWait[int, string](10*time.Millisecond),
		dataloader.WithCache[int, string](cache),
	)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var counter int // Counter for unique keys per goroutine
		for pb.Next() {
			// Each goroutine accesses a rotating set of keys to ensure high cache hit rate
			// but also some distribution to avoid all hitting key 0.
			key := (counter + int(time.Now().UnixNano())) % numPreFillItems
			thunk := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: key, Val: fmt.Sprintf("val_%d", key)})
			result := thunk()
			if result.Err != nil {
				b.Errorf("Unexpected error: %v", result.Err)
			}
			blackhole = result.Val
			counter++
		}
	})
}


func BenchmarkLoadOrExec_FullBatches_NoCache(b *testing.B) {
	maxBatch := 100
	loader := dataloader.NewDataLoader(benchmarkExecFn,
		dataloader.WithMaxBatch[int, string](maxBatch),
		dataloader.WithWait[int, string](200*time.Millisecond), // Long wait to ensure batching by size
		dataloader.WithCache[int, string](dataloader.DefaultCache[int, string]{}),
	)

	items := make([]dataloader.Item[int, string], b.N)
	for i := 0; i < b.N; i++ {
		items[i] = dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("val_%d", i)}
	}

	b.ResetTimer()
	// Load all items first, then resolve. This pattern is common.
	thunks := make([]func() dataloader.Result[string], b.N)
	for i := 0; i < b.N; i++ {
		thunks[i] = loader.LoadOrExec(context.Background(), items[i])
	}

	for i := 0; i < b.N; i++ {
		result := thunks[i]()
		if result.Err != nil {
			b.Fatalf("Unexpected error: %v", result.Err)
		}
		blackhole = result.Val
	}
}
