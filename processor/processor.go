// Package processor is helper for async tasks processing with retries and TTL
package processor

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reddec/slowqueue/queue"
)

// HandlerFunc is a transaction handler for the element in the queue.
type HandlerFunc func(ctx context.Context, stream io.ReadSeeker) error

// Config for the processor. Use Default for reasonable values.
type Config struct {
	Retry   time.Duration // interval between attempts
	Backoff time.Duration // interval between attempts in case of queue failure
	TTL     time.Duration // maximum processing time since first enqueue (time in queue and in retry)
	Dir     string        // queues location
}

// Default configuration: 10s for retry, TTL 1 hour, backoff 5s, location is "data"
func Default() Config {
	return Config{
		Retry:   10 * time.Second,
		Backoff: 5 * time.Second,
		TTL:     time.Hour,
		Dir:     "data",
	}
}

// New instance of Processor. Creates or loads automatically required files.
func New(cfg Config) (*Processor, error) {
	main, err := queue.New(filepath.Join(cfg.Dir, "main"))
	if err != nil {
		return nil, fmt.Errorf("create main queue: %w", err)
	}
	retry, err := queue.New(filepath.Join(cfg.Dir, "retry"))
	if err != nil {
		return nil, fmt.Errorf("create retry queue: %w", err)
	}
	return &Processor{
		main:     main,
		retry:    retry,
		interval: cfg.Retry,
		backoff:  cfg.Backoff,
		ttl:      cfg.TTL,
	}, nil
}

// Processor is additional level on top of SlowQueue: it's designed for processing queue elements in background with
// retries if needed.
// Basically, processor creates two queue: one for tasks, one for retries (with fixed retry interval). Processor will retry
// to process queue element until element expired (configured by TTL).
// Due to access to more than one queue, it's possible to get duplicated elements during hard abortion on re-queue phase.
type Processor struct {
	main     *queue.SlowQueue
	retry    *queue.SlowQueue
	interval time.Duration
	backoff  time.Duration
	ttl      time.Duration
	guard    int32
}

// Put element into the tasks (main) queue. Element will be processed asap.
func (r *Processor) Put(stream io.Reader) error {
	return r.Transaction(func(writer io.Writer) error {
		_, err := io.Copy(writer, stream)
		return err
	})
}

// Transaction block guarantees that in case it returns without error, queue will have new element at the end with content
// that user provided into writer.
func (r *Processor) Transaction(tx func(writer io.Writer) error) error {
	return r.main.Transaction(func(writer io.Writer) error {
		now := time.Now()
		if err := binary.Write(writer, binary.BigEndian, header{
			Enqueued: now.UnixMilli(),
		}); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		return tx(writer)
	})
}

// Run processing and re-queueing logic. It will exit only when context finished.
// In case of underlying error (ex: disk failure) it will retry process message again and aging with Config.Backoff interval.
// It is not allowed to call Run in parallel. Only first call will run, others will stop immediately.
func (r *Processor) Run(ctx context.Context, handler HandlerFunc) {
	if !atomic.CompareAndSwapInt32(&r.guard, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&r.guard, 0)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := r.processMain(ctx, handler)
			if err == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.backoff):
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := r.dequeueDelayed(ctx)
			if err == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.backoff):
			}
		}
	}()

	wg.Wait()
}

func (r *Processor) processMain(ctx context.Context, handler HandlerFunc) error {
	stream, err := r.main.Get(ctx)
	if err != nil {
		return fmt.Errorf("get from main queue: %w", err)
	}
	defer stream.Close()

	var h header
	if err := binary.Read(stream, binary.BigEndian, &h); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// discard old message
	if time.Since(time.UnixMilli(h.Enqueued)) > r.ttl {
		return r.main.Discard()
	}
	section := io.NewSectionReader(stream, int64(binary.Size(h)), stream.Size())
	err = handler(ctx, section)
	if err == nil {
		return r.main.Discard()
	}

	if _, err := stream.Seek(int64(binary.Size(h)), 0); err != nil {
		return fmt.Errorf("reset main stream: %w", err)
	}

	err = r.retry.Transaction(func(writer io.Writer) error {
		h.LastAttemptAt = time.Now().UnixMilli()
		if err := binary.Write(writer, binary.BigEndian, h); err != nil {
			return fmt.Errorf("encode header: %w", err)
		}
		if _, err := io.Copy(writer, stream); err != nil {
			return fmt.Errorf("copy message: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return r.main.Discard()
}

func (r *Processor) dequeueDelayed(ctx context.Context) error {
	stream, err := r.retry.Get(ctx)
	if err != nil {
		return fmt.Errorf("get from retry queue: %w", err)
	}
	defer stream.Close()

	var h header
	if err := binary.Read(stream, binary.BigEndian, &h); err != nil {
		return fmt.Errorf("parse header: %w", err)
	}

	// discard old message
	if time.Since(time.UnixMilli(h.Enqueued)) > r.ttl {
		return r.retry.Discard()
	}

	if _, err := stream.Seek(0, 0); err != nil {
		return fmt.Errorf("reset stream: %w", err)
	}

	enqueuedAt := time.UnixMilli(h.LastAttemptAt)
	delta := time.Since(enqueuedAt)
	if delta < r.interval {
		select {
		case <-time.After(r.interval - delta):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// enqueue back (stream already contains header)
	if err := r.main.Put(stream); err != nil {
		return fmt.Errorf("enqueue back: %w", err)
	}

	return r.retry.Discard()
}

type header struct {
	Enqueued      int64 // unix ts ms
	LastAttemptAt int64 // unix ts ms
}
