package model

import "fmt"

type Side uint8

const (
	SideBuy Side = iota + 1
	SideSell
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "BUY"
	case SideSell:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

type CommandType uint8

const (
	CommandNew CommandType = iota + 1
	CommandCancel
	CommandReplace
)

type OrderType uint8

const (
	OrderTypeLimit OrderType = iota + 1
	OrderTypeMarket
)

type TimeInForce uint8

const (
	TIFGTC TimeInForce = iota + 1
	TIFIOC
	TIFFOK
)

type RejectReason uint16

const (
	RejectNone RejectReason = iota
	RejectInvalidSymbol
	RejectInvalidPrice
	RejectInvalidQuantity
	RejectDuplicateOrderID
	RejectUnknownOrderID
	RejectInvalidReplace
	RejectWouldNotFullyFill
	RejectInvalidOrderID
	RejectInvalidSide
	RejectInvalidOrderType
	RejectInvalidTimeInForce
	RejectInvalidCommandType
)

type CancelReason uint8

const (
	CancelByUser CancelReason = iota + 1
	CancelUnfilledRemainder
)

type EventType uint8

const (
	EventAccepted EventType = iota + 1
	EventRejected
	EventTrade
	EventOrderReduced
	EventOrderRested
	EventCanceled
	EventReplaced
)

type Command struct {
	Seq          uint64
	Symbol       string
	Type         CommandType
	OrderID      uint64
	Side         Side
	OrderType    OrderType
	TimeInForce  TimeInForce
	Price        int64
	Quantity     int64
	NewPrice     int64
	NewLeavesQty int64
}

func (c Command) ValidationRejectReason() RejectReason {
	if c.Symbol == "" {
		return RejectInvalidSymbol
	}

	switch c.Type {
	case CommandNew:
		if c.OrderID == 0 {
			return RejectInvalidOrderID
		}
		if c.Side != SideBuy && c.Side != SideSell {
			return RejectInvalidSide
		}
		if c.OrderType != OrderTypeLimit && c.OrderType != OrderTypeMarket {
			return RejectInvalidOrderType
		}
		if c.TimeInForce != TIFGTC && c.TimeInForce != TIFIOC && c.TimeInForce != TIFFOK {
			return RejectInvalidTimeInForce
		}
		if c.Quantity <= 0 {
			return RejectInvalidQuantity
		}
		if c.OrderType == OrderTypeLimit && c.Price <= 0 {
			return RejectInvalidPrice
		}
	case CommandCancel:
		if c.OrderID == 0 {
			return RejectInvalidOrderID
		}
	case CommandReplace:
		if c.OrderID == 0 {
			return RejectInvalidOrderID
		}
		if c.NewLeavesQty <= 0 || c.NewPrice < 0 {
			return RejectInvalidReplace
		}
	default:
		return RejectInvalidCommandType
	}
	return RejectNone
}

func (c Command) Validate() error {
	if c.Symbol == "" {
		return fmt.Errorf("invalid symbol")
	}

	switch c.Type {
	case CommandNew:
		if c.OrderID == 0 {
			return fmt.Errorf("order id is required")
		}
		if c.Side != SideBuy && c.Side != SideSell {
			return fmt.Errorf("invalid side")
		}
		if c.OrderType != OrderTypeLimit && c.OrderType != OrderTypeMarket {
			return fmt.Errorf("invalid order type")
		}
		if c.TimeInForce != TIFGTC && c.TimeInForce != TIFIOC && c.TimeInForce != TIFFOK {
			return fmt.Errorf("invalid tif")
		}
		if c.Quantity <= 0 {
			return fmt.Errorf("invalid quantity")
		}
		if c.OrderType == OrderTypeLimit && c.Price <= 0 {
			return fmt.Errorf("invalid price")
		}
	case CommandCancel:
		if c.OrderID == 0 {
			return fmt.Errorf("order id is required")
		}
	case CommandReplace:
		if c.OrderID == 0 {
			return fmt.Errorf("order id is required")
		}
		if c.NewLeavesQty <= 0 {
			return fmt.Errorf("invalid replace quantity")
		}
		if c.NewPrice < 0 {
			return fmt.Errorf("invalid replace price")
		}
	default:
		return fmt.Errorf("invalid command type")
	}

	return nil
}

type Event struct {
	Type             EventType
	Reason           RejectReason
	OrderID          uint64
	MakerOrderID     uint64
	TakerOrderID     uint64
	TradeID          uint64
	Price            int64
	Quantity         int64
	LeavesQty        int64
	CanceledQty      int64
	CancelReason     CancelReason
	PriorityRetained bool
}

type Result struct {
	CommandSeq uint64
	Events     []Event
}

type Outcome struct {
	Result Result
	Err    error
}
