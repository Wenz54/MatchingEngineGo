package journal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"petProjectMatchingEngine/internal/metrics"
)

var ErrWriterClosed = errors.New("wal writer is closed")

type WriterConfig struct {
	Dir              string
	EngineID         string
	ShardID          int
	ShardCount       int
	QueueSize        int
	MaxBatchCommands int
	MaxBatchBytes    int
	MaxBatchDelay    time.Duration
	SegmentMaxBytes  int64
	Metrics          metrics.Sink
}

func (w *Writer) Flush(ctx context.Context) error {
	if err := w.getTerminalError(); err != nil {
		return err
	}

	done := make(chan struct{})
	req := writerRequest{kind: writerReqFlush, done: done}
	select {
	case w.requests <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-done:
		return w.getTerminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type appendRequest struct {
	record Record
	ack    chan error
}

type writerRequestKind uint8

const (
	writerReqAppend writerRequestKind = iota + 1
	writerReqFlush
	writerReqShutdown
)

type writerRequest struct {
	kind   writerRequestKind
	append appendRequest
	done   chan struct{}
}

type Writer struct {
	cfg WriterConfig

	requests chan writerRequest
	done     chan struct{}

	lifecycleMu sync.RWMutex
	mu          sync.RWMutex
	closed      bool
	failedErr   error
}

func NewWriter(cfg WriterConfig) (*Writer, error) {
	if err := validateWriterConfig(cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	w := &Writer{
		cfg:      cfg,
		requests: make(chan writerRequest, cfg.QueueSize),
		done:     make(chan struct{}),
	}
	go w.run()
	return w, nil
}

func (w *Writer) Append(ctx context.Context, rec Record) error {
	ack, err := w.AppendAsync(ctx, rec)
	if err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) AppendAsync(ctx context.Context, rec Record) (<-chan error, error) {
	w.lifecycleMu.RLock()
	defer w.lifecycleMu.RUnlock()
	if err := w.getTerminalError(); err != nil {
		return nil, err
	}
	if w.isClosed() {
		return nil, ErrWriterClosed
	}

	ack := make(chan error, 1)
	req := writerRequest{kind: writerReqAppend, append: appendRequest{record: rec, ack: ack}}
	select {
	case w.requests <- req:
		return ack, nil
	case <-w.done:
		if err := w.getTerminalError(); err != nil {
			return nil, err
		}
		return nil, ErrWriterClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *Writer) Shutdown(ctx context.Context) error {
	w.lifecycleMu.Lock()
	if !w.markClosed() {
		w.lifecycleMu.Unlock()
		select {
		case <-w.done:
			return w.getTerminalError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	req := writerRequest{kind: writerReqShutdown, done: done}
	select {
	case w.requests <- req:
		w.lifecycleMu.Unlock()
	case <-w.done:
		w.lifecycleMu.Unlock()
		return w.getTerminalError()
	case <-ctx.Done():
		w.reopen()
		w.lifecycleMu.Unlock()
		return ctx.Err()
	}

	select {
	case <-done:
		return w.getTerminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) run() {
	defer close(w.done)

	state := &writerState{w: w}
	if err := state.openNextSegment(); err != nil {
		w.setTerminalError(err)
		w.rejectRemaining(err)
		return
	}
	defer state.closeCurrent()

	timer := time.NewTimer(w.cfg.MaxBatchDelay)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	timerArmed := false

	pendingBytes := make([]byte, 0, w.cfg.MaxBatchBytes*2)
	pendingAcks := make([]chan error, 0, w.cfg.MaxBatchCommands)
	flush := func() bool {
		if len(pendingAcks) == 0 {
			return true
		}
		batchCommands := len(pendingAcks)
		batchBytes := len(pendingBytes)
		start := time.Now()
		if err := state.writeBatch(pendingBytes); err != nil {
			w.setTerminalError(err)
			for _, ack := range pendingAcks {
				ack <- err
			}
			w.rejectRemaining(err)
			return false
		}
		for _, ack := range pendingAcks {
			ack <- nil
		}
		if w.cfg.Metrics != nil {
			w.cfg.Metrics.ObserveWALBatch(batchCommands, batchBytes)
			w.cfg.Metrics.ObserveWALSyncLatency(time.Since(start))
		}
		pendingBytes = pendingBytes[:0]
		pendingAcks = pendingAcks[:0]
		if timerArmed {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerArmed = false
		}
		return true
	}

	for {
		var timerC <-chan time.Time
		if timerArmed {
			timerC = timer.C
		}

		select {
		case req := <-w.requests:
			switch req.kind {
			case writerReqAppend:
				var err error
				pendingBytes, err = appendEncodedRecord(pendingBytes, req.append.record)
				if err != nil {
					req.append.ack <- err
					continue
				}
				pendingAcks = append(pendingAcks, req.append.ack)
				if !timerArmed {
					timer.Reset(w.cfg.MaxBatchDelay)
					timerArmed = true
				}

				if len(pendingAcks) >= w.cfg.MaxBatchCommands || len(pendingBytes) >= w.cfg.MaxBatchBytes {
					if ok := flush(); !ok {
						return
					}
				}
			case writerReqFlush:
				if ok := flush(); !ok {
					close(req.done)
					return
				}
				close(req.done)
			case writerReqShutdown:
				_ = flush()
				close(req.done)
				return
			}
		case <-timerC:
			if ok := flush(); !ok {
				return
			}
		}
	}
}

func (w *Writer) rejectRemaining(err error) {
	for {
		select {
		case req := <-w.requests:
			switch req.kind {
			case writerReqAppend:
				req.append.ack <- err
			case writerReqFlush:
				close(req.done)
			case writerReqShutdown:
				close(req.done)
			}
		default:
			return
		}
	}
}

func (w *Writer) setTerminalError(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failedErr == nil {
		w.failedErr = err
	}
}

func (w *Writer) getTerminalError() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.failedErr
}

func (w *Writer) isClosed() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.closed
}

func (w *Writer) markClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.closed = true
	return true
}

func (w *Writer) reopen() {
	w.mu.Lock()
	w.closed = false
	w.mu.Unlock()
}

func validateWriterConfig(cfg WriterConfig) error {
	if cfg.Dir == "" {
		return fmt.Errorf("wal dir is required")
	}
	if cfg.EngineID == "" {
		return fmt.Errorf("engine id is required")
	}
	if cfg.ShardID < 0 || cfg.ShardID >= cfg.ShardCount {
		return fmt.Errorf("invalid shard id")
	}
	if cfg.QueueSize <= 0 || cfg.MaxBatchCommands <= 0 || cfg.MaxBatchBytes <= 0 {
		return fmt.Errorf("invalid queue or batch config")
	}
	if cfg.MaxBatchDelay <= 0 || cfg.SegmentMaxBytes <= 0 {
		return fmt.Errorf("invalid delay or segment size")
	}
	return nil
}

type writerState struct {
	w            *Writer
	segmentIndex int
	currentFile  *os.File
	currentSize  int64
}

func (s *writerState) openNextSegment() error {
	if s.segmentIndex == 0 {
		next, err := nextSegmentIndex(s.w.cfg.Dir, s.w.cfg.ShardID)
		if err != nil {
			return err
		}
		s.segmentIndex = next
	}

	if s.currentFile != nil {
		if err := s.currentFile.Close(); err != nil {
			return err
		}
		s.currentFile = nil
		s.currentSize = 0
	}

	path := filepath.Join(s.w.cfg.Dir, fmt.Sprintf("shard-%02d-%06d.wal", s.w.cfg.ShardID, s.segmentIndex))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	header, err := EncodeSegmentHeader(SegmentHeader{
		EngineID:   s.w.cfg.EngineID,
		ShardID:    uint16(s.w.cfg.ShardID),
		ShardCount: uint16(s.w.cfg.ShardCount),
	})
	if err != nil {
		_ = f.Close()
		return err
	}

	n, err := f.Write(header)
	if err != nil {
		_ = f.Close()
		return err
	}
	if n != len(header) {
		_ = f.Close()
		return fmt.Errorf("short write on segment header")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := syncDirectory(s.w.cfg.Dir); err != nil {
		_ = f.Close()
		return err
	}

	s.currentFile = f
	s.currentSize = int64(len(header))
	s.segmentIndex++
	return nil
}

func (s *writerState) writeBatch(batch []byte) error {
	if len(batch) == 0 {
		return nil
	}

	if s.currentSize+int64(len(batch)) > s.w.cfg.SegmentMaxBytes {
		if err := s.openNextSegment(); err != nil {
			return err
		}
	}

	n, err := s.currentFile.Write(batch)
	if err != nil {
		return err
	}
	if n != len(batch) {
		return fmt.Errorf("short write in wal batch")
	}
	if err := s.currentFile.Sync(); err != nil {
		return err
	}
	s.currentSize += int64(n)
	return nil
}

func (s *writerState) closeCurrent() {
	if s.currentFile != nil {
		_ = s.currentFile.Close()
	}
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func nextSegmentIndex(dir string, shardID int) (int, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("shard-%02d-*.wal", shardID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, nil
	}

	sort.Strings(files)
	last := filepath.Base(files[len(files)-1])
	parts := strings.Split(last, "-")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid wal segment name: %s", last)
	}
	idxPart := strings.TrimSuffix(parts[2], ".wal")
	idx, err := strconv.Atoi(idxPart)
	if err != nil {
		return 0, fmt.Errorf("invalid wal segment index: %s", last)
	}
	return idx + 1, nil
}
