package async

import (
	"context"
	"time"

	"github.com/semka95/batch-processing/service"
)

// AsyncClient represents asynchronous client interface
type AsyncClient interface {
	SendData(data service.Batch)
	Start()
	Stop()
}

// ErrorBatch represents error returned by Process function of the Service.
// It contains error and batch, that failed to be processed.
type ErrorBatch struct {
	err   error
	batch service.Batch
}

type client struct {
	batchSize  int
	interval   time.Duration
	service    service.Service
	dataCh     chan service.Item
	doneCh     chan struct{}
	errBatchCh chan ErrorBatch
}

// New creates new asynchronous client
func New(s service.Service, dataChBufSize, errChBufSize int) (AsyncClient, chan ErrorBatch) {
	n, p := s.GetLimits()
	dataCh := make(chan service.Item, dataChBufSize)
	doneCh := make(chan struct{})
	errCh := make(chan ErrorBatch, errChBufSize)

	return &client{
		batchSize:  int(n),
		interval:   p,
		service:    s,
		dataCh:     dataCh,
		doneCh:     doneCh,
		errBatchCh: errCh,
	}, errCh
}

// SendData sends Items to channel to be processed
func (c *client) SendData(data service.Batch) {
	for _, v := range data {
		c.dataCh <- v
	}
}

// Start starts sending Batches to Service. It collects Items from channel, creates Batch and sends
// it to the Service. If fumction collects too few Items for given interval, it sends all collected
// Items to Service. After every send operation function creates new Batch, resets timer and
// updates current time. If function recieves signal from done channel, it sends all available Items
// in Batch to Service and quits.
func (c *client) Start() {
	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	batch, now := c.prepareSend(timer)

	for {
		if len(batch) == c.batchSize {
			time.Sleep(c.interval - time.Since(now))
			c.processBatch(batch)
			batch, now = c.prepareSend(timer)
			continue
		}
		select {
		case item, ok := <-c.dataCh:
			if ok {
				batch = append(batch, item)
			}
		case <-timer.C:
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			batch, now = c.prepareSend(timer)
		case <-c.doneCh:
			time.Sleep(c.interval - time.Since(now))
			c.processBatch(batch)
			return
		}
	}
}

// processBatch sends Batch to the Service. If there are any errors, it sends ErrorBatch
// to the error channel
func (c *client) processBatch(batch service.Batch) {
	err := c.service.Process(context.Background(), batch)
	if err != nil {
		errB := make(service.Batch, len(batch))
		copy(errB, batch)
		c.errBatchCh <- ErrorBatch{
			err:   err,
			batch: errB,
		}
	}
}

// prepareSend creates new timer, new Batch, and current time
func (c *client) prepareSend(timer *time.Timer) (service.Batch, time.Time) {
	timer.Stop()
	timer.Reset(c.interval)
	return make(service.Batch, 0, c.batchSize), time.Now()
}

// Stop sends command to stop sending Batches to Service
func (c *client) Stop() {
	c.doneCh <- struct{}{}
}
