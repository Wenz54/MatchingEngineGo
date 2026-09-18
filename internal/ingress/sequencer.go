package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"petProjectMatchingEngine/internal/metrics"
	"petProjectMatchingEngine/internal/model"
)

var ErrBackpressure = errors.New("backpressure: ingress queue is full")
var ErrSequencerClosed = errors.New("sequencer is closed")
var ErrSequenceExhausted = errors.New("command sequence exhausted")

type RoutedKind uint8

const (
	RoutedCommand RoutedKind = iota + 1
	RoutedBarrier
	RoutedDrain
)

type Routed struct {
	Kind        RoutedKind
	Command     model.Command
	Reply       chan model.Outcome
	BarrierDone chan error
	DrainAck    chan error
	DrainWait   <-chan struct{}
}

type Router interface {
	ShardFor(symbol string) (int, bool)
	ShardCount() int
}

type requestKind uint8

const (
	requestCommand requestKind = iota + 1
	requestBarrier
	requestDrain
	requestShutdown
)

type request struct {
	kind    requestKind
	command model.Command
	reply   chan model.Outcome
	done    chan struct{}
	result  chan error
	release chan struct{}
}

type Sequencer struct {
	router       Router
	shardInboxes []chan<- Routed
	ingressQ     chan request
	done         chan struct{}
	metrics      metrics.Sink
	nextSeq      uint64

	mu     sync.RWMutex
	closed bool
}

func NewSequencer(queueSize int, initialSequence uint64, shardInboxes []chan<- Routed, router Router, sink metrics.Sink) *Sequencer {
	if sink == nil {
		sink = metrics.NoopSink{}
	}
	if initialSequence == 0 {
		initialSequence = 1
	}
	s := &Sequencer{
		router:       router,
		shardInboxes: shardInboxes,
		ingressQ:     make(chan request, queueSize),
		done:         make(chan struct{}),
		metrics:      sink,
		nextSeq:      initialSequence,
	}
	go s.run()
	return s
}

func (s *Sequencer) SubmitBlocking(ctx context.Context, cmd model.Command, reply chan model.Outcome) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSequencerClosed
	}

	req := request{kind: requestCommand, command: cmd, reply: reply}
	select {
	case s.ingressQ <- req:
		s.metrics.ObserveIngressQueueDepth(len(s.ingressQ))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sequencer) SubmitNonBlocking(cmd model.Command, reply chan model.Outcome) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSequencerClosed
	}

	req := request{kind: requestCommand, command: cmd, reply: reply}
	select {
	case s.ingressQ <- req:
		s.metrics.ObserveIngressQueueDepth(len(s.ingressQ))
		return nil
	default:
		s.metrics.IncBackpressure()
		return ErrBackpressure
	}
}

func (s *Sequencer) Barrier(ctx context.Context) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrSequencerClosed
	}

	result := make(chan error, 1)
	req := request{kind: requestBarrier, result: result}

	select {
	case s.ingressQ <- req:
		s.mu.RUnlock()
	case <-ctx.Done():
		s.mu.RUnlock()
		return ctx.Err()
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sequencer) Shutdown(ctx context.Context) error {
	if !s.markClosed() {
		select {
		case <-s.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	req := request{kind: requestShutdown, done: done}

	select {
	case s.ingressQ <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sequencer) AcquireDrain(ctx context.Context) (func(), error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrSequencerClosed
	}

	result := make(chan error, 1)
	release := make(chan struct{})
	req := request{kind: requestDrain, result: result, release: release}

	select {
	case s.ingressQ <- req:
		s.mu.RUnlock()
	case <-ctx.Done():
		s.mu.RUnlock()
		return nil, ctx.Err()
	}

	select {
	case err := <-result:
		if err != nil {
			close(release)
			return nil, err
		}
		return func() { close(release) }, nil
	case <-ctx.Done():
		close(release)
		return nil, ctx.Err()
	}
}

func (s *Sequencer) run() {
	defer close(s.done)

	for req := range s.ingressQ {
		switch req.kind {
		case requestCommand:
			cmd := req.command
			shardID, ok := s.router.ShardFor(cmd.Symbol)
			if !ok {
				s.metrics.IncRejected()
				req.reply <- model.Outcome{Result: model.Result{
					CommandSeq: 0,
					Events: []model.Event{{
						Type:    model.EventRejected,
						Reason:  model.RejectInvalidSymbol,
						OrderID: cmd.OrderID,
					}},
				}}
				continue
			}

			if s.nextSeq == 0 {
				req.reply <- model.Outcome{Err: ErrSequenceExhausted}
				continue
			}
			cmd.Seq = s.nextSeq
			s.nextSeq++
			s.metrics.ObserveShardQueueDepth(len(s.shardInboxes[shardID]))
			s.shardInboxes[shardID] <- Routed{Kind: RoutedCommand, Command: cmd, Reply: req.reply}
		case requestBarrier:
			var firstErr error
			for _, inbox := range s.shardInboxes {
				barrierDone := make(chan error, 1)
				inbox <- Routed{Kind: RoutedBarrier, BarrierDone: barrierDone}
				if err := <-barrierDone; err != nil && firstErr == nil {
					firstErr = err
				}
			}
			req.result <- firstErr
		case requestDrain:
			var firstErr error
			for _, inbox := range s.shardInboxes {
				drainAck := make(chan error, 1)
				inbox <- Routed{Kind: RoutedDrain, DrainAck: drainAck, DrainWait: req.release}
				if err := <-drainAck; err != nil && firstErr == nil {
					firstErr = err
				}
			}
			req.result <- firstErr
			<-req.release
		case requestShutdown:
			for _, inbox := range s.shardInboxes {
				close(inbox)
			}
			close(req.done)
			return
		default:
			panic(fmt.Sprintf("unknown sequencer request kind: %d", req.kind))
		}
	}
}

func (s *Sequencer) markClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	return true
}
