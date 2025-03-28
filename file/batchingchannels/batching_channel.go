package batchingchannels

import (
	"context"
	"errors"

	"github.com/askiada/external-sort/vector"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// BatchingChannel implements the Channel interface, with the change that instead of producing individual elements
// on Out(), it batches together the entire internal buffer each time. Trying to construct an unbuffered batching channel
// will panic, that configuration is not supported (and provides no benefit over an unbuffered NativeChannel).
type BatchingChannel struct {
	input    chan string
	output   chan vector.Vector
	buffer   vector.Vector
	allocate *vector.Allocate
	g        *errgroup.Group
	sem      *semaphore.Weighted
	dCtx     context.Context
	size     int
}

// NewBatchingChannel returns a BatchingChannel with max workers. It creates a
// goroutine and will stop it when the context is cancelled. It returns an
// error if the input is invalid.
func NewBatchingChannel(ctx context.Context, allocate *vector.Allocate, maxWorker int64, size int) (*BatchingChannel, error) {
	if size == 0 {
		return nil, errors.New("channels: BatchingChannel does not support unbuffered behaviour")
	}
	if size < 0 {
		return nil, errors.New("channels: invalid negative size in NewBatchingChannel")
	}
	g, dCtx := errgroup.WithContext(ctx)
	ch := &BatchingChannel{
		input:    make(chan string),
		output:   make(chan vector.Vector),
		size:     size,
		allocate: allocate,
		g:        g,
		sem:      semaphore.NewWeighted(maxWorker),
		dCtx:     dCtx,
	}
	go ch.batchingBuffer(ctx)
	return ch, nil
}

func (ch *BatchingChannel) In() chan<- string {
	return ch.input
}

// Out returns a <-chan vector.Vector in order that BatchingChannel conforms to the standard Channel interface provided
// by this package, however each output value is guaranteed to be of type vector.Vector - a vector collecting the most
// recent batch of values sent on the In channel. The vector is guaranteed to not be empty or nil.
func (ch *BatchingChannel) Out() <-chan vector.Vector {
	return ch.output
}

func (ch *BatchingChannel) ProcessOut(f func(vector.Vector) error) error {
	for val := range ch.Out() {
		if err := ch.sem.Acquire(ch.dCtx, 1); err != nil {
			return err
		}
		val := val
		ch.g.Go(func() error {
			defer ch.sem.Release(1)
			return f(val)
		})
	}
	err := ch.g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (ch *BatchingChannel) Len() int {
	return ch.size
}

func (ch *BatchingChannel) Cap() int {
	return ch.size
}

func (ch *BatchingChannel) Close() {
	close(ch.input)
}

func (ch *BatchingChannel) batchingBuffer(ctx context.Context) {
	ch.buffer = ch.allocate.Vector(ch.size, ch.allocate.Key)
	defer close(ch.output)
	for elem := range ch.input {
		select {
		case <-ctx.Done():
			ch.g.Go(func() error {
				return ctx.Err()
			})
			return
		default:
		}
		err := ch.buffer.PushBack(elem)
		if err != nil {
			ch.g.Go(func() error {
				return err
			})
		}
		if ch.buffer.Len() == ch.size {
			ch.output <- ch.buffer
			ch.buffer = ch.allocate.Vector(ch.size, ch.allocate.Key)
		}
	}
	if ch.buffer.Len() > 0 {
		ch.output <- ch.buffer
	}
}
