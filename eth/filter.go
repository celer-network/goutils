package eth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
)

// WrapFilter loop calls filterFunc with the block range not exceeding maxBlockDelta. Retries with half the block range
// on error until all blocks are covered, or the range has shrunk to a single block and filterFunc still returns error.
func WrapFilter(fromBlock, finalBlock, maxBlockDelta uint64, filterFunc func(f *bind.FilterOpts) error) error {
	createFilterOpts := func(start, end uint64, ctx context.Context) *bind.FilterOpts {
		if maxBlockDelta > 0 {
			end = min(start+maxBlockDelta, end)
		}

		return &bind.FilterOpts{
			Start:   start,
			End:     &end,
			Context: ctx,
		}
	}
	currStart := fromBlock
	currEnd := finalBlock
	for {
		ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer cancel()

		filterOpts := createFilterOpts(currStart, currEnd, ctx)
		err := filterFunc(filterOpts)
		if err != nil {
			// If retryable error and range not exhausted, halve the range and retry
			if isRetryableFetchError(err) && (filterOpts.Start < *filterOpts.End) {
				currEnd = filterOpts.Start + (*filterOpts.End-filterOpts.Start)/2
			} else {
				return fmt.Errorf("filterFunc err: %w", err)
			}
		} else {
			if *filterOpts.End == finalBlock {
				break
			}
			// Try to query up to final block again
			currStart = *filterOpts.End + 1
			currEnd = finalBlock
		}
		time.Sleep(100 * time.Millisecond) // Avoid hitting the RPC too hard
	}
	return nil
}

func isRetryableFetchError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "more than") ||
		strings.Contains(errStr, "Block range") ||
		strings.Contains(errStr, "block range") ||
		strings.Contains(errStr, "request timed out") ||
		strings.Contains(errStr, "query cancelled") ||
		strings.Contains(errStr, "temporarily") ||
		strings.Contains(errStr, "Internal") ||
		strings.Contains(errStr, "internal")
}
