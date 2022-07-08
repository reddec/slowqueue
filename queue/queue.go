// Package queue providing file-based FIFO queue
package queue

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ErrEmptyQueue used for signaling of attempt read empty queue.
var ErrEmptyQueue = errors.New("empty queue")

// DataStream represents stored, read-only content of queue element.
type DataStream interface {
	io.ReadSeekCloser
	io.ReaderAt
	Size() int64
}

// New instance of queue.
// May fail in case old content exists but not available or broken.
func New(dir string) (*SlowQueue, error) {
	sq := &SlowQueue{
		dir:   dir,
		check: make(chan struct{}, 1),
	}
	return sq, sq.unsafeReadInfo()
}

// SlowQueue is classical FIFO queue, based on approach where one file is one element.
// Designed to store as minimal as possible information in memory. Number of elements and
// element sized only limited by underlying file storage.
// Does NOT support access to the same queue from different processes, however, it is go-routine safe.
// SlowQueue designed to handle a lot of large elements by with reasonable compromise of speed in memory-constraint
// environments.
type SlowQueue struct {
	dir   string
	info  info
	lock  sync.Mutex
	check chan struct{}
}

// Directory where information stored.
func (sq *SlowQueue) Directory() string {
	return sq.dir
}

// Get existent (oldest) element from the queue or wait till context finished or new element added.
func (sq *SlowQueue) Get(ctx context.Context) (DataStream, error) {
	for {
		s, err := sq.Poll()
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrEmptyQueue) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-sq.check:
		}
	}
}

// Poll oldest element from the queue. Returns ErrEmptyQueue in case no more element left.
func (sq *SlowQueue) Poll() (DataStream, error) {
	sq.lock.Lock()
	defer sq.lock.Unlock()
	if sq.empty() {
		return nil, ErrEmptyQueue
	}
	f, err := os.Open(sq.path(sq.info.Head))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat: %w", err)
	}

	return &fileDataStream{
		File: f,
		size: stat.Size(),
	}, nil
}

// Discard (commit) oldest element from the queue. May return ErrEmptyQueue if queue is empty.
func (sq *SlowQueue) Discard() error {
	id, err, removed := sq.discardHead()
	if err != nil || !removed {
		return err
	}
	// remove head file
	headFile := sq.path(id)
	return os.RemoveAll(headFile)
}

// Put data into the tail of the queue. This operation guarantees consistency regardless of kind of interruption, but may
// generate junk files in case of rough abortion.
// The operation is just convenient wrapper around Transaction.
func (sq *SlowQueue) Put(stream io.Reader) error {
	return sq.Transaction(func(writer io.Writer) error {
		_, err := io.Copy(writer, stream)
		return err
	})
}

// Info of the queue (snapshot).
func (sq *SlowQueue) Info() Stat {
	sq.lock.Lock()
	defer sq.lock.Unlock()
	return Stat{Size: sq.info.Tail - sq.info.Head}
}

// Transaction provides writer for the last element of the queue.
// This operation guarantees consistency regardless of kind of interruption, but may
// generate junk files in case of rough abortion.
// Transaction can be called from the parallel go-routines in any order, however, append order will be the same as
// finishing order (end of TX block).
// Note: writer object MUST not be used outside the transaction scope.
func (sq *SlowQueue) Transaction(tx func(writer io.Writer) error) error {
	if err := os.MkdirAll(sq.tempDir(), 0755); err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	tmpFile, err := ioutil.TempFile(sq.tempDir(), "")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.RemoveAll(tmpFile.Name())
	defer tmpFile.Close()

	if err := tx(tmpFile); err != nil {
		return fmt.Errorf("transaction: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := sq.atomicAppend(tmpFile.Name()); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	sq.notify()
	return nil
}

// let others know that we have something to check
func (sq *SlowQueue) notify() {
	select {
	case sq.check <- struct{}{}:
	default:
	}
}

// atomically rename file to the tail ID. Updates queue info file.
func (sq *SlowQueue) atomicAppend(tmpFile string) error {
	sq.lock.Lock()
	defer sq.lock.Unlock()

	id := sq.info.Tail
	file := sq.path(id)
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	if err := os.Rename(tmpFile, file); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	sq.info.Tail++
	if err := sq.unsafeSaveInfo(); err != nil {
		sq.info.Tail--
		return fmt.Errorf("save info: %w", err)
	}

	return nil
}

// initial queue load if files are exist.
func (sq *SlowQueue) unsafeReadInfo() error {
	var info info
	f, err := os.Open(sq.infoFile())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read info: %w", err)
	}
	defer f.Close()

	if err := binary.Read(f, binary.BigEndian, &info); err != nil {
		return fmt.Errorf("decode info: %w", err)
	}

	sq.info = info
	return nil
}

// atomically write queue info.
func (sq *SlowQueue) unsafeSaveInfo() error {
	if err := os.MkdirAll(filepath.Dir(sq.infoFile()), 0755); err != nil {
		return fmt.Errorf("create info dir: %w", err)
	}
	var cache bytes.Buffer
	if err := binary.Write(&cache, binary.BigEndian, sq.info); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	tmpFile := sq.infoFile() + ".tmp"
	defer os.RemoveAll(tmpFile)

	if err := ioutil.WriteFile(tmpFile, cache.Bytes(), 0755); err != nil {
		return fmt.Errorf("write temp info: %w", err)
	}
	return os.Rename(tmpFile, sq.infoFile())
}

// remove oldest queue element and file.
func (sq *SlowQueue) discardHead() (uint64, error, bool) {
	sq.lock.Lock()
	defer sq.lock.Unlock()
	if sq.empty() {
		return 0, ErrEmptyQueue, false
	}
	id := sq.info.Head
	sq.info.Head++

	// commit first info
	if err := sq.unsafeSaveInfo(); err != nil {
		sq.info.Head--
		return 0, fmt.Errorf("update info: %w", err), false
	}
	return id, nil, true
}

func (sq *SlowQueue) path(id uint64) string {
	return filepath.Join(sq.dir, path(id))
}

func (sq *SlowQueue) tempDir() string {
	return filepath.Join(sq.dir, "temp")
}

func (sq *SlowQueue) infoFile() string {
	return filepath.Join(sq.dir, "info.bin")
}

func (sq *SlowQueue) empty() bool {
	// write to tail, get from head
	return sq.info.Empty()
}

type info struct {
	Head uint64
	Tail uint64
}

func (info info) Empty() bool {
	return info.Head >= info.Tail
}

type Stat struct {
	Size uint64
}

// naive hierarchy of files to prevent overloading of each folder.
// 1'000'000 files will fit into no more than 1000 files per directory. After that, top level directory will grow.
func path(id uint64) string {
	root := id / 1000
	base := root / 1000
	return filepath.Join(strconv.FormatUint(base, 10), strconv.FormatUint(root, 10), strconv.FormatUint(id, 10))
}

type fileDataStream struct {
	*os.File
	size int64
}

func (fds *fileDataStream) Size() int64 {
	return fds.size
}
