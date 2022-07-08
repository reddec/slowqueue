package queue_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/reddec/slowqueue/queue"
)

func TestNew(t *testing.T) {
	t.Run("general flow is working", func(t *testing.T) {
		q := newTempQueue(t)
		if err := q.Put(bytes.NewBufferString("hello world")); err != nil {
			t.Fatal(err)
		}
		r, err := q.Poll()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "hello world" {
			t.Error(string(data))
		}

		if err := q.Discard(); err != nil {
			t.Fatal(err)
		}
		_, err = q.Poll()
		if !errors.Is(err, queue.ErrEmptyQueue) {
			t.Error(err)
		}
	})
	t.Run("cycled read-write", func(t *testing.T) {
		q := newTempQueue(t)
		for i := 0; i < 10; i++ {
			if err := q.Put(bytes.NewReader([]byte(strconv.Itoa(i)))); err != nil {
				t.Fatal(err)
			}
			data, err := pollBytes(q)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != strconv.Itoa(i) {
				t.Error(string(data))
			}

			if err := q.Discard(); err != nil {
				t.Fatal(err)
			}
		}
		_, err := q.Poll()
		if !errors.Is(err, queue.ErrEmptyQueue) {
			t.Error(err)
		}
	})
	t.Run("unbalanced cycled read-write", func(t *testing.T) {
		q := newTempQueue(t)
		for i := 0; i < 10; i++ {
			if err := q.Put(bytes.NewReader([]byte(strconv.Itoa(i)))); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			data, err := pollBytes(q)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != strconv.Itoa(i) {
				t.Error(string(data))
			}

			if err := q.Discard(); err != nil {
				t.Fatal(err)
			}
		}
		_, err := pollBytes(q)
		if !errors.Is(err, queue.ErrEmptyQueue) {
			t.Error(err)
		}
	})
	t.Run("discard empty", func(t *testing.T) {
		q := newTempQueue(t)
		if err := q.Put(bytes.NewReader([]byte(strconv.Itoa(123)))); err != nil {
			t.Fatal(err)
		}
		data, err := pollBytes(q)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != strconv.Itoa(123) {
			t.Error(string(data))
		}

		if err := q.Discard(); err != nil {
			t.Fatal(err)
		}
		err = q.Discard()
		if !errors.Is(err, queue.ErrEmptyQueue) {
			t.Error(err)
		}
	})
	t.Run("open-close", func(t *testing.T) {
		q := newTempQueue(t)
		if err := q.Put(bytes.NewReader([]byte(strconv.Itoa(123)))); err != nil {
			t.Fatal(err)
		}
		b, err := queue.New(q.Directory())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v, %+v", q.Info(), b.Info())
		data, err := pollBytes(b)
		if err != nil {
			t.Fatal(err, q.Directory(), b.Directory())
		}
		if string(data) != strconv.Itoa(123) {
			t.Error(string(data))
		}
	})
}

func newTempQueue(t *testing.T) *queue.SlowQueue {
	d, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(d)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func pollBytes(q *queue.SlowQueue) ([]byte, error) {
	s, err := q.Poll()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	return ioutil.ReadAll(s)
}

func BenchmarkNew(b *testing.B) {
	d, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatal(err)
	}
	q, err := queue.New(d)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.Run("put-poll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := q.Put(bytes.NewReader([]byte("hello world"))); err != nil {
				b.Fatal(err)
			}
			if data, err := pollBytes(q); err != nil || string(data) != "hello world" {
				b.Fatal(err)
			}
		}
	})
}
