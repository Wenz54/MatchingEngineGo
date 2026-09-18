package book

import (
	"fmt"

	"petProjectMatchingEngine/internal/model"
)

type Order struct {
	ID        uint64
	Side      model.Side
	Price     int64
	LeavesQty int64
	Seq       uint64
	Prev      *Order
	Next      *Order
	Level     *PriceLevel
}

type PriceLevel struct {
	Price    int64
	TotalQty int64
	Head     *Order
	Tail     *Order
}

type sideBook struct {
	levels map[int64]*PriceLevel
	prices []int64
	isBid  bool
}

func newSideBook(isBid bool) *sideBook {
	return &sideBook{
		levels: make(map[int64]*PriceLevel),
		prices: make([]int64, 0, 64),
		isBid:  isBid,
	}
}

func (s *sideBook) best() *PriceLevel {
	if len(s.prices) == 0 {
		return nil
	}
	price := s.prices[0]
	return s.levels[price]
}

func (s *sideBook) getOrCreateLevel(price int64) *PriceLevel {
	if level, ok := s.levels[price]; ok {
		return level
	}
	level := &PriceLevel{Price: price}
	s.levels[price] = level
	idx := s.findInsertIndex(price)
	s.prices = append(s.prices, 0)
	copy(s.prices[idx+1:], s.prices[idx:])
	s.prices[idx] = price
	return level
}

func (s *sideBook) removeLevelIfEmpty(level *PriceLevel) {
	if level == nil || level.Head != nil {
		return
	}
	delete(s.levels, level.Price)
	idx := s.findPriceIndex(level.Price)
	if idx >= 0 {
		s.prices = append(s.prices[:idx], s.prices[idx+1:]...)
	}
}

func (s *sideBook) findPriceIndex(price int64) int {
	idx := s.findInsertIndex(price)
	if idx >= len(s.prices) || s.prices[idx] != price {
		return -1
	}
	return idx
}

func (s *sideBook) findInsertIndex(price int64) int {
	left, right := 0, len(s.prices)
	for left < right {
		mid := left + (right-left)/2
		if s.isBid {
			if s.prices[mid] > price {
				left = mid + 1
			} else {
				right = mid
			}
		} else {
			if s.prices[mid] < price {
				left = mid + 1
			} else {
				right = mid
			}
		}
	}
	return left
}

func (l *PriceLevel) append(order *Order) {
	order.Prev = l.Tail
	order.Next = nil
	order.Level = l
	if l.Tail != nil {
		l.Tail.Next = order
	} else {
		l.Head = order
	}
	l.Tail = order
	l.TotalQty += order.LeavesQty
}

func (l *PriceLevel) remove(order *Order) {
	if order.Prev != nil {
		order.Prev.Next = order.Next
	} else {
		l.Head = order.Next
	}
	if order.Next != nil {
		order.Next.Prev = order.Prev
	} else {
		l.Tail = order.Prev
	}
	l.TotalQty -= order.LeavesQty
	order.Prev = nil
	order.Next = nil
	order.Level = nil
}

type Book struct {
	Symbol       string
	bids         *sideBook
	asks         *sideBook
	orders       map[uint64]*Order
	nextTradeSeq uint64
}

type Snapshot struct {
	Symbol       string
	NextTradeSeq uint64
	Bids         []PriceLevelSnapshot
	Asks         []PriceLevelSnapshot
}

type PriceLevelSnapshot struct {
	Price  int64
	Orders []OrderSnapshot
}

type OrderSnapshot struct {
	ID        uint64
	Side      model.Side
	Price     int64
	LeavesQty int64
	Seq       uint64
}

func New(symbol string) *Book {
	return &Book{
		Symbol:       symbol,
		bids:         newSideBook(true),
		asks:         newSideBook(false),
		orders:       make(map[uint64]*Order),
		nextTradeSeq: 1,
	}
}

func (b *Book) Apply(cmd model.Command) model.Result {
	result := model.Result{CommandSeq: cmd.Seq, Events: make([]model.Event, 0, 8)}

	if cmd.Symbol != b.Symbol {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectInvalidSymbol, OrderID: cmd.OrderID})
		return result
	}
	if err := cmd.Validate(); err != nil {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: cmd.ValidationRejectReason(), OrderID: cmd.OrderID})
		return result
	}

	switch cmd.Type {
	case model.CommandNew:
		b.handleNew(cmd, &result)
	case model.CommandCancel:
		b.handleCancel(cmd, &result)
	case model.CommandReplace:
		b.handleReplace(cmd, &result)
	}

	return result
}

func (b *Book) handleNew(cmd model.Command, result *model.Result) {
	if _, exists := b.orders[cmd.OrderID]; exists {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectDuplicateOrderID, OrderID: cmd.OrderID})
		return
	}

	if cmd.TimeInForce == model.TIFFOK {
		if b.availableQtyFor(cmd.Side, cmd.Price, cmd.OrderType, cmd.Quantity) < cmd.Quantity {
			result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectWouldNotFullyFill, OrderID: cmd.OrderID})
			return
		}
	}

	result.Events = append(result.Events, model.Event{Type: model.EventAccepted, OrderID: cmd.OrderID})

	order := &Order{ID: cmd.OrderID, Side: cmd.Side, Price: cmd.Price, LeavesQty: cmd.Quantity, Seq: cmd.Seq}
	b.match(order, result)

	if order.LeavesQty == 0 {
		return
	}

	if cmd.OrderType == model.OrderTypeMarket {
		result.Events = append(result.Events, model.Event{Type: model.EventCanceled, OrderID: order.ID, CanceledQty: order.LeavesQty, CancelReason: model.CancelUnfilledRemainder})
		return
	}

	if cmd.TimeInForce == model.TIFIOC {
		result.Events = append(result.Events, model.Event{Type: model.EventCanceled, OrderID: order.ID, CanceledQty: order.LeavesQty, CancelReason: model.CancelUnfilledRemainder})
		return
	}

	b.rest(order)
	result.Events = append(result.Events, model.Event{Type: model.EventOrderRested, OrderID: order.ID, Price: order.Price, LeavesQty: order.LeavesQty})
}

func (b *Book) handleCancel(cmd model.Command, result *model.Result) {
	order, ok := b.orders[cmd.OrderID]
	if !ok {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectUnknownOrderID, OrderID: cmd.OrderID})
		return
	}

	result.Events = append(result.Events, model.Event{Type: model.EventAccepted, OrderID: cmd.OrderID})

	canceled := order.LeavesQty
	b.removeActive(order)
	result.Events = append(result.Events, model.Event{Type: model.EventCanceled, OrderID: cmd.OrderID, CanceledQty: canceled, CancelReason: model.CancelByUser})
}

func (b *Book) handleReplace(cmd model.Command, result *model.Result) {
	order, ok := b.orders[cmd.OrderID]
	if !ok {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectUnknownOrderID, OrderID: cmd.OrderID})
		return
	}

	newPrice := order.Price
	if cmd.NewPrice > 0 {
		newPrice = cmd.NewPrice
	}
	if newPrice <= 0 {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectInvalidReplace, OrderID: cmd.OrderID})
		return
	}

	newLeaves := cmd.NewLeavesQty
	if newLeaves <= 0 {
		result.Events = append(result.Events, model.Event{Type: model.EventRejected, Reason: model.RejectInvalidReplace, OrderID: cmd.OrderID})
		return
	}

	result.Events = append(result.Events, model.Event{Type: model.EventAccepted, OrderID: cmd.OrderID})

	retainPriority := newPrice == order.Price && newLeaves <= order.LeavesQty
	if retainPriority {
		reduced := order.LeavesQty - newLeaves
		order.Level.TotalQty -= reduced
		order.LeavesQty = newLeaves
		if reduced > 0 {
			result.Events = append(result.Events, model.Event{Type: model.EventOrderReduced, OrderID: order.ID, LeavesQty: order.LeavesQty})
		}
		result.Events = append(result.Events, model.Event{Type: model.EventReplaced, OrderID: order.ID, Price: order.Price, LeavesQty: order.LeavesQty, PriorityRetained: true})
		return
	}

	side := order.Side
	b.removeActive(order)

	replaced := &Order{ID: cmd.OrderID, Side: side, Price: newPrice, LeavesQty: newLeaves, Seq: cmd.Seq}
	b.match(replaced, result)

	if replaced.LeavesQty > 0 {
		b.rest(replaced)
		result.Events = append(result.Events, model.Event{Type: model.EventOrderRested, OrderID: replaced.ID, Price: replaced.Price, LeavesQty: replaced.LeavesQty})
	}

	result.Events = append(result.Events, model.Event{Type: model.EventReplaced, OrderID: cmd.OrderID, Price: newPrice, LeavesQty: replaced.LeavesQty, PriorityRetained: false})
}

func (b *Book) rest(order *Order) {
	bookSide := b.bids
	if order.Side == model.SideSell {
		bookSide = b.asks
	}
	level := bookSide.getOrCreateLevel(order.Price)
	level.append(order)
	b.orders[order.ID] = order
}

func (b *Book) match(incoming *Order, result *model.Result) {
	if incoming.Side == model.SideBuy {
		b.matchAgainst(incoming, b.asks, func(bestPrice int64) bool {
			if incoming.Price == 0 {
				return true
			}
			return bestPrice <= incoming.Price
		}, result)
		return
	}

	b.matchAgainst(incoming, b.bids, func(bestPrice int64) bool {
		if incoming.Price == 0 {
			return true
		}
		return bestPrice >= incoming.Price
	}, result)
}

func (b *Book) matchAgainst(incoming *Order, opposite *sideBook, priceAllowed func(int64) bool, result *model.Result) {
	for incoming.LeavesQty > 0 {
		best := opposite.best()
		if best == nil || !priceAllowed(best.Price) {
			return
		}

		resting := best.Head
		tradeQty := min64(incoming.LeavesQty, resting.LeavesQty)
		incoming.LeavesQty -= tradeQty
		resting.LeavesQty -= tradeQty
		resting.Level.TotalQty -= tradeQty

		trade := model.Event{
			Type:         model.EventTrade,
			TradeID:      b.nextTradeID(result.CommandSeq),
			MakerOrderID: resting.ID,
			TakerOrderID: incoming.ID,
			Price:        resting.Price,
			Quantity:     tradeQty,
		}
		result.Events = append(result.Events, trade)

		if resting.LeavesQty == 0 {
			b.removeActive(resting)
		} else {
			result.Events = append(result.Events, model.Event{Type: model.EventOrderReduced, OrderID: resting.ID, LeavesQty: resting.LeavesQty})
		}
	}
}

func (b *Book) nextTradeID(commandSeq uint64) uint64 {
	id := (commandSeq << 20) | (b.nextTradeSeq & ((1 << 20) - 1))
	b.nextTradeSeq++
	return id
}

func (b *Book) removeActive(order *Order) {
	if order.Level == nil {
		return
	}
	level := order.Level
	bookSide := b.bids
	if order.Side == model.SideSell {
		bookSide = b.asks
	}
	level.remove(order)
	bookSide.removeLevelIfEmpty(level)
	delete(b.orders, order.ID)
}

func (b *Book) availableQtyFor(side model.Side, limitPrice int64, orderType model.OrderType, requiredQty int64) int64 {
	var opposite *sideBook
	var canTake func(int64) bool

	if side == model.SideBuy {
		opposite = b.asks
		canTake = func(price int64) bool {
			if orderType == model.OrderTypeMarket {
				return true
			}
			return price <= limitPrice
		}
	} else {
		opposite = b.bids
		canTake = func(price int64) bool {
			if orderType == model.OrderTypeMarket {
				return true
			}
			return price >= limitPrice
		}
	}

	var total int64
	for _, price := range opposite.prices {
		if !canTake(price) {
			break
		}
		levelQty := opposite.levels[price].TotalQty
		if levelQty >= requiredQty-total {
			return requiredQty
		}
		total += levelQty
	}
	return total
}

func (b *Book) CheckInvariants() error {
	for _, side := range []*sideBook{b.bids, b.asks} {
		for i := 1; i < len(side.prices); i++ {
			if side.isBid && side.prices[i] > side.prices[i-1] {
				return fmt.Errorf("bids are not sorted")
			}
			if !side.isBid && side.prices[i] < side.prices[i-1] {
				return fmt.Errorf("asks are not sorted")
			}
		}

		for _, price := range side.prices {
			level := side.levels[price]
			if level == nil {
				return fmt.Errorf("missing level")
			}
			if level.Head == nil || level.Tail == nil {
				return fmt.Errorf("empty level in index")
			}

			var qty int64
			for o := level.Head; o != nil; o = o.Next {
				if o.Level != level {
					return fmt.Errorf("order-level backref mismatch")
				}
				if o.LeavesQty <= 0 {
					return fmt.Errorf("non-positive leaves")
				}
				qty += o.LeavesQty
			}
			if qty != level.TotalQty {
				return fmt.Errorf("total qty mismatch")
			}
		}
	}

	if b.bids.best() != nil && b.asks.best() != nil && b.bids.best().Price >= b.asks.best().Price {
		return fmt.Errorf("crossed book detected")
	}

	return nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (b *Book) Snapshot() Snapshot {
	return Snapshot{
		Symbol:       b.Symbol,
		NextTradeSeq: b.nextTradeSeq,
		Bids:         snapshotSide(b.bids),
		Asks:         snapshotSide(b.asks),
	}
}

func NewFromSnapshot(s Snapshot) *Book {
	b := New(s.Symbol)
	b.nextTradeSeq = s.NextTradeSeq

	restoreSide := func(levels []PriceLevelSnapshot) {
		for _, lvl := range levels {
			for _, ord := range lvl.Orders {
				order := &Order{
					ID:        ord.ID,
					Side:      ord.Side,
					Price:     ord.Price,
					LeavesQty: ord.LeavesQty,
					Seq:       ord.Seq,
				}
				b.rest(order)
			}
		}
	}

	restoreSide(s.Bids)
	restoreSide(s.Asks)
	return b
}

func snapshotSide(side *sideBook) []PriceLevelSnapshot {
	out := make([]PriceLevelSnapshot, 0, len(side.prices))
	for _, price := range side.prices {
		level := side.levels[price]
		orders := make([]OrderSnapshot, 0, 8)
		for o := level.Head; o != nil; o = o.Next {
			orders = append(orders, OrderSnapshot{
				ID:        o.ID,
				Side:      o.Side,
				Price:     o.Price,
				LeavesQty: o.LeavesQty,
				Seq:       o.Seq,
			})
		}
		out = append(out, PriceLevelSnapshot{Price: price, Orders: orders})
	}
	return out
}
