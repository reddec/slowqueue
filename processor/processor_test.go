package processor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/reddec/slowqueue/processor"
)

func TestNew(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cfg := processor.Default()
	cfg.Retry = time.Second
	cfg.Dir = dir

	p, err := processor.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts int

	if err := p.Put(bytes.NewBufferString("hello world")); err != nil {
		t.Error(err)
	}

	var last time.Time

	p.Run(ctx, func(_ context.Context, stream io.ReadSeeker) error {
		data, err := ioutil.ReadAll(stream)
		if err != nil {
			return err
		}
		if string(data) != "hello world" {
			t.Error(string(data))
		}
		if delta := time.Since(last); delta < cfg.Retry {
			t.Error("too fast retry:", delta)
		}
		last = time.Now()
		attempts++
		if attempts < 3 {
			return fmt.Errorf("oops")
		}
		cancel()
		return nil
	})
	if attempts != 3 {
		t.Error(attempts)
	}
}
