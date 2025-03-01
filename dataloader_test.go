package dataloader_test

import (
	"context"
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
		name     string
		execFunc func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error)
		input    []dataloader.Item[int, string]
		expected map[int]string
		options  []dataloader.Option[int, string]
	}{
		{
			name: "TestAdd",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				errs := make(map[int]error)
				for _, d := range data {
					d.Val = d.Val + "_processed"
					res[d.Key] = d.Val
					errs[d.Key] = nil
				}
				return res, errs
			},
			input: []dataloader.Item[int, string]{
				{
					Key: 1,
					Val: "1_test",
				},
				{
					Key: 2,
					Val: "2_test",
				},
			},
			expected: map[int]string{
				1: "1_test_processed",
				2: "2_test_processed",
			},
		},
		{
			name: "TestAddExecWithMaxBatch",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				errs := make(map[int]error)
				for _, d := range data {
					d.Val = d.Val + "_processed"
					res[d.Key] = d.Val
					errs[d.Key] = nil
				}
				return res, errs
			},
			input: []dataloader.Item[int, string]{
				{
					Key: 1,
					Val: "1_test",
				},
				{
					Key: 2,
					Val: "2_test",
				},
				{
					Key: 3,
					Val: "3_test",
				},
				{
					Key: 4,
					Val: "4_test",
				},
				{
					Key: 5,
					Val: "5_test",
				},
			},
			expected: map[int]string{
				1: "1_test_processed",
				2: "2_test_processed",
				3: "3_test_processed",
				4: "4_test_processed",
				5: "5_test_processed",
			},
			options: []dataloader.Option[int, string]{
				dataloader.WithMaxBatch[int, string](2),
				dataloader.WithWait[int, string](50 * time.Millisecond),
			},
		},
		{
			name: "TestExecFuncError",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				errs := make(map[int]error)
				for _, d := range data {
					errs[d.Key] = fmt.Errorf("execFunc error")
				}
				return res, errs
			},
			input: []dataloader.Item[int, string]{
				{Key: 1, Val: "1_test"},
			},
			expected: map[int]string{},
		},
		{
			name: "TestLargeInput",
			execFunc: func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
				res := make(map[int]string)
				errs := make(map[int]error)
				for _, d := range data {
					d.Val = d.Val + "_processed"
					res[d.Key] = d.Val
					errs[d.Key] = nil
				}
				return res, errs
			},
			input: func() []dataloader.Item[int, string] {
				var input []dataloader.Item[int, string]
				for i := range 10000 {
					input = append(input, dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("%d_test", i)})
				}
				return input
			}(),
			expected: func() map[int]string {
				expected := make(map[int]string)
				for i := range 10000 {
					expected[i] = fmt.Sprintf("%d_test_processed", i)
				}
				return expected
			}(),
			options: []dataloader.Option[int, string]{
				dataloader.WithMaxBatch[int, string](2000),
			},
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			suite.T().Parallel()
			batcher := dataloader.NewDataLoader(tc.execFunc, tc.options...)
			_, expectedErr := tc.execFunc(context.Background(), tc.input)
			actual := make(map[int]func() dataloader.Result[string])
			for _, input := range tc.input {
				thunk := batcher.LoadOrExec(context.Background(), input)
				actual[input.Key] = thunk
			}
			for key, thunk := range actual {
				val := thunk()
				if expectedErr[key] != nil {
					suite.EqualError(expectedErr[key], val.Err.Error())
				} else {
					suite.NoError(val.Err)
					suite.Equal(tc.expected[key], val.Val)
				}
			}
		})
	}
}

func (suite *LoaderTestSuite) TestCachePanic() {
	cache := &mockopt.MockPanicCache[int, string]{}
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		res := make(map[int]string)
		errs := make(map[int]error)
		for _, d := range data {
			res[d.Key] = d.Val
			errs[d.Key] = nil
		}
		return res, errs
	}
	loader := dataloader.NewDataLoader(execFn, dataloader.WithCache(cache))
	cache.On("Get", 1).Return("", false).Once()
	result := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: 1, Val: "1_test"})
	suite.ErrorContains(result().Err, dataloader.ErrPanicAddValInCache.Error())
}

func (suite *LoaderTestSuite) TestLoadOrExecPanic() {
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		panic("execFn panic")
	}
	loader := dataloader.NewDataLoader(execFn)
	result := loader.LoadOrExec(context.Background(), dataloader.Item[int, string]{Key: 1})
	suite.ErrorContains(result().Err, dataloader.ErrPanicBatch.Error())
}

func (suite *LoaderTestSuite) TestConcurrency() {
	total := 1000
	result := make(chan func() dataloader.Result[string], total)
	execFn := func(ctx context.Context, data []dataloader.Item[int, string]) (map[int]string, map[int]error) {
		res := make(map[int]string)
		errs := make(map[int]error)
		for _, d := range data {
			res[d.Key] = d.Val + "_processed"
			errs[d.Key] = nil
		}
		return res, errs
	}
	inputs := make([]dataloader.Item[int, string], 0, total)
	for i := range total {
		inputs = append(inputs, dataloader.Item[int, string]{Key: i, Val: fmt.Sprintf("%d", i)})
	}
	resp, _ := execFn(context.Background(), inputs)
	expected := make(map[string]struct{})
	for _, v := range resp {
		expected[v] = struct{}{}
	}

	loader := dataloader.NewDataLoader(execFn)
	var wg sync.WaitGroup
	for _, input := range inputs {
		wg.Add(1)
		go func(input dataloader.Item[int, string]) {
			defer wg.Done()
			result <- loader.LoadOrExec(context.Background(), input)
		}(input)
	}
	wg.Wait()
	close(result)

	for item := range result {
		val := item()
		suite.NoError(val.Err)
		if _, ok := expected[val.Val]; !ok {
			suite.Fail("unexpected value")
		}
		delete(expected, val.Val)
	}
	suite.Empty(expected)
}
