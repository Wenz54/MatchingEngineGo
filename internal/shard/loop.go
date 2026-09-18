package shard

import (
	"context"
	"fmt"
	"sort"
	"time"

	"petProjectMatchingEngine/internal/book"
	"petProjectMatchingEngine/internal/ingress"
	"petProjectMatchingEngine/internal/journal"
	"petProjectMatchingEngine/internal/metrics"
	"petProjectMatchingEngine/internal/model"
)

type walWriter interface {
	AppendAsync(context.Context, journal.Record) (<-chan error, error)
	Flush(context.Context) error
	Shutdown(context.Context) error
}

type Loop struct {
	id             int
	books          map[string]*book.Book
	inbox          <-chan ingress.Routed
	done           chan struct{}
	wal            walWriter
	lastAppliedSeq uint64
	failedErr      error
	metrics        metrics.Sink
}

func newLoop(id int, symbols []string, inbox <-chan ingress.Routed, wal walWriter, snapshots map[string]book.Snapshot, sink metrics.Sink) *Loop {
	if sink == nil {
		sink = metrics.NoopSink{}
	}

	books := make(map[string]*book.Book, len(symbols))
	for _, symbol := range symbols {
		if snap, ok := snapshots[symbol]; ok {
			books[symbol] = book.NewFromSnapshot(snap)
			continue
		}
		books[symbol] = book.New(symbol)
	}

	return &Loop{
		id:      id,
		books:   books,
		inbox:   inbox,
		done:    make(chan struct{}),
		wal:     wal,
		metrics: sink,
	}
}

func (l *Loop) Start() {
	go l.run()
}

func (l *Loop) Done() <-chan struct{} {
	return l.done
}

func (l *Loop) snapshots() []book.Snapshot {
	syms := make([]string, 0, len(l.books))
	for sym := range l.books {
		syms = append(syms, sym)
	}
	sort.Strings(syms)

	out := make([]book.Snapshot, 0, len(l.books))
	for _, sym := range syms {
		out = append(out, l.books[sym].Snapshot())
	}
	return out
}

func (l *Loop) lastApplied() uint64 {
	return l.lastAppliedSeq
}

func (l *Loop) run() {
	defer close(l.done)
	defer func() {
		if l.wal == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.wal.Shutdown(ctx); err != nil && l.failedErr == nil {
			l.failedErr = fmt.Errorf("wal shutdown failed on shard %d: %w", l.id, err)
		}
	}()

	const maxPendingCommands = 1024
	inbox := l.inbox
	pending := make([]pendingCommand, 0, maxPendingCommands)
	for inbox != nil || len(pending) > 0 {
		var nextAck <-chan error
		if len(pending) > 0 {
			nextAck = pending[0].ack
		}
		receive := inbox
		if len(pending) >= maxPendingCommands {
			receive = nil
		}

		select {
		case msg, ok := <-receive:
			if !ok {
				inbox = nil
				continue
			}
			switch msg.Kind {
			case ingress.RoutedCommand:
				l.enqueueCommand(msg, &pending)
			case ingress.RoutedBarrier:
				msg.BarrierDone <- l.flushPending(&pending)
			case ingress.RoutedDrain:
				msg.DrainAck <- l.flushPending(&pending)
				<-msg.DrainWait
			}
		case err := <-nextAck:
			l.completeNext(&pending, err)
		}
	}
}

type pendingCommand struct {
	sequence uint64
	result   model.Result
	reply    chan model.Outcome
	ack      <-chan error
}

func (l *Loop) enqueueCommand(msg ingress.Routed, pending *[]pendingCommand) {
	if l.failedErr != nil {
		msg.Reply <- model.Outcome{Err: l.failedErr}
		return
	}
	bookForSymbol, ok := l.books[msg.Command.Symbol]
	if !ok {
		result := model.Result{
			CommandSeq: msg.Command.Seq,
			Events: []model.Event{{
				Type:    model.EventRejected,
				Reason:  model.RejectInvalidSymbol,
				OrderID: msg.Command.OrderID,
			}},
		}
		l.metrics.IncRejected()
		msg.Reply <- model.Outcome{Result: result}
		return
	}

	result := bookForSymbol.Apply(msg.Command)
	if l.wal == nil {
		l.completeCommand(msg.Command.Seq, result, msg.Reply)
		return
	}

	record := journal.Record{
		ShardID:    uint16(l.id),
		CommandSeq: msg.Command.Seq,
		Command:    msg.Command,
		Result:     result,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ack, err := l.wal.AppendAsync(ctx, record)
	cancel()
	if err != nil {
		l.failPending(pending, fmt.Errorf("wal append failed on shard %d: %w", l.id, err))
		msg.Reply <- model.Outcome{Err: l.failedErr}
		return
	}
	*pending = append(*pending, pendingCommand{sequence: msg.Command.Seq, result: result, reply: msg.Reply, ack: ack})
}

func (l *Loop) completeNext(pending *[]pendingCommand, err error) {
	command := (*pending)[0]
	*pending = (*pending)[1:]
	if err != nil {
		l.failedErr = fmt.Errorf("wal append failed on shard %d: %w", l.id, err)
		command.reply <- model.Outcome{Err: l.failedErr}
		l.failPending(pending, l.failedErr)
		return
	}
	l.completeCommand(command.sequence, command.result, command.reply)
}

func (l *Loop) completeAll(pending *[]pendingCommand) {
	for len(*pending) > 0 {
		err := <-(*pending)[0].ack
		l.completeNext(pending, err)
	}
}

func (l *Loop) flushPending(pending *[]pendingCommand) error {
	if err := l.flushWAL(); err != nil {
		l.failPending(pending, err)
		return err
	}
	l.completeAll(pending)
	return l.failedErr
}

func (l *Loop) failPending(pending *[]pendingCommand, err error) {
	l.failedErr = err
	for _, command := range *pending {
		command.reply <- model.Outcome{Err: err}
	}
	*pending = (*pending)[:0]
}

func (l *Loop) completeCommand(sequence uint64, result model.Result, reply chan model.Outcome) {
	l.lastAppliedSeq = sequence
	if hasRejected(result) {
		l.metrics.IncRejected()
	} else {
		l.metrics.IncAccepted()
	}
	reply <- model.Outcome{Result: result}
}

func (l *Loop) flushWAL() error {
	if l.failedErr != nil {
		return l.failedErr
	}
	if l.wal == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := l.wal.Flush(ctx); err != nil {
		l.failedErr = fmt.Errorf("wal flush failed on shard %d: %w", l.id, err)
	}
	return l.failedErr
}

func hasRejected(result model.Result) bool {
	for _, event := range result.Events {
		if event.Type == model.EventRejected {
			return true
		}
	}
	return false
}
