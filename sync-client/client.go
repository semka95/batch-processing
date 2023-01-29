package sync_client

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/semka95/batch-processing/service"
)

// SyncClient represents synchronous client interface
type SyncClient interface {
	Send(data []service.Item, returnResidual bool) (int, error)
}

type cl struct {
	batchSize int
	interval  time.Duration
	service   service.Service
	mu        sync.Mutex
}

// New creates new synchronous client
func New(s service.Service) SyncClient {
	n, p := s.GetLimits()
	return &cl{
		batchSize: int(n),
		interval:  p,
		service:   s,
		mu:        sync.Mutex{},
	}
}

// Send splits up slice of Items to batches and sends them to the external service.
// This function fits the case when there is a big chunk of data available. There is
// only one function can be run at the same time. If number of Items is not evenly divisible
// by batch size function sends last batch not fully filled. You can set returnResidual to true,
// so residual Items would not be sent and index of first residual Item would be returned.
// Send returns length of Items if there is no error or index of the first Item of the batch
// if there is error.
func (c *cl) Send(data []service.Item, returnResidual bool) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	skip := 0
	itemsAmount := len(data)
	batchAmount := int(math.Ceil(float64(itemsAmount / c.batchSize)))

	for i := 0; i < batchAmount; i++ {
		lowerBound := skip
		upperBound := skip + c.batchSize

		if upperBound > itemsAmount {
			upperBound = itemsAmount
			if returnResidual {
				return lowerBound, nil
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.interval)
		err := c.service.Process(ctx, data[lowerBound:upperBound])
		if err != nil {
			cancel()
			return lowerBound, fmt.Errorf("can't send batch from Item %d to Item %d: %w", lowerBound, upperBound, err)
		}

		skip += c.batchSize

		time.Sleep(c.interval)
		cancel()
	}

	return itemsAmount, nil
}
