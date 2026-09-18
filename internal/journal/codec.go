package journal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"petProjectMatchingEngine/internal/model"
)

const (
	segmentMagic   uint32 = 0x53474C57
	recordMagic    uint32 = 0x52434C57
	schemaVersion  uint16 = 1
	recordTypeExec uint8  = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	ShardID    uint16
	CommandSeq uint64
	Command    model.Command
	Result     model.Result
}

type SegmentHeader struct {
	EngineID   string
	ShardID    uint16
	ShardCount uint16
}

func EncodeSegmentHeader(h SegmentHeader) ([]byte, error) {
	if len(h.EngineID) > 0xffff {
		return nil, fmt.Errorf("engine id too long")
	}

	payloadLen := 2 + len(h.EngineID)
	buf := bytes.NewBuffer(make([]byte, 0, 12+payloadLen))
	_ = binary.Write(buf, binary.LittleEndian, segmentMagic)
	_ = binary.Write(buf, binary.LittleEndian, schemaVersion)
	_ = binary.Write(buf, binary.LittleEndian, h.ShardCount)
	_ = binary.Write(buf, binary.LittleEndian, h.ShardID)
	_ = binary.Write(buf, binary.LittleEndian, uint16(len(h.EngineID)))
	_, _ = buf.WriteString(h.EngineID)
	return buf.Bytes(), nil
}

func DecodeSegmentHeader(data []byte) (SegmentHeader, int, error) {
	r := bytes.NewReader(data)
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return SegmentHeader{}, 0, err
	}
	if magic != segmentMagic {
		return SegmentHeader{}, 0, fmt.Errorf("invalid segment magic")
	}
	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return SegmentHeader{}, 0, err
	}
	if version != schemaVersion {
		return SegmentHeader{}, 0, fmt.Errorf("unsupported schema version: %d", version)
	}

	var shardCount, shardID, engineIDLen uint16
	if err := binary.Read(r, binary.LittleEndian, &shardCount); err != nil {
		return SegmentHeader{}, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &shardID); err != nil {
		return SegmentHeader{}, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &engineIDLen); err != nil {
		return SegmentHeader{}, 0, err
	}
	engineID := make([]byte, engineIDLen)
	if _, err := io.ReadFull(r, engineID); err != nil {
		return SegmentHeader{}, 0, err
	}

	consumed := len(data) - r.Len()
	return SegmentHeader{EngineID: string(engineID), ShardID: shardID, ShardCount: shardCount}, consumed, nil
}

func EncodeRecord(rec Record) ([]byte, error) {
	return appendEncodedRecord(nil, rec)
}

func appendEncodedRecord(dst []byte, rec Record) ([]byte, error) {
	payloadLen, err := payloadSize(rec)
	if err != nil {
		return dst, err
	}

	const headerLen = 24
	frameLen := headerLen + payloadLen + 4
	start := len(dst)
	if cap(dst)-start >= frameLen {
		dst = dst[:start+frameLen]
	} else {
		dst = append(dst, make([]byte, frameLen)...)
	}
	buf := dst[start:]
	binary.LittleEndian.PutUint32(buf[0:4], recordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], schemaVersion)
	buf[6] = recordTypeExec
	buf[7] = 0
	binary.LittleEndian.PutUint32(buf[8:12], uint32(payloadLen))
	binary.LittleEndian.PutUint16(buf[12:14], rec.ShardID)
	binary.LittleEndian.PutUint16(buf[14:16], 0)
	binary.LittleEndian.PutUint64(buf[16:24], rec.CommandSeq)
	encodePayload(buf[headerLen:headerLen+payloadLen], rec)
	binary.LittleEndian.PutUint32(buf[frameLen-4:], crc32.Checksum(buf[:frameLen-4], crcTable))
	return dst, nil
}

func DecodeRecord(data []byte) (Record, int, error) {
	const headerLen = 24
	if len(data) < headerLen {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	if magic := binary.LittleEndian.Uint32(data[0:4]); magic != recordMagic {
		return Record{}, 0, fmt.Errorf("invalid record magic")
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != schemaVersion {
		return Record{}, 0, fmt.Errorf("unsupported schema version: %d", version)
	}
	if recType := data[6]; recType != recordTypeExec {
		return Record{}, 0, fmt.Errorf("unsupported record type: %d", recType)
	}

	payloadLen := uint64(binary.LittleEndian.Uint32(data[8:12]))
	required64 := uint64(headerLen) + payloadLen + 4
	if required64 > uint64(len(data)) {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	required := int(required64)
	frame := data[:required]
	want := binary.LittleEndian.Uint32(frame[required-4:])
	if got := crc32.Checksum(frame[:required-4], crcTable); got != want {
		return Record{}, 0, fmt.Errorf("checksum mismatch")
	}

	rec := Record{
		ShardID:    binary.LittleEndian.Uint16(data[12:14]),
		CommandSeq: binary.LittleEndian.Uint64(data[16:24]),
	}
	if err := decodePayload(frame[headerLen:required-4], &rec); err != nil {
		return Record{}, 0, err
	}
	return rec, required, nil
}

const (
	payloadFixedSize = 58
	eventEncodedSize = 69
)

func payloadSize(rec Record) (int, error) {
	if len(rec.Command.Symbol) > 0xffff {
		return 0, fmt.Errorf("symbol too long")
	}
	payloadLen := uint64(payloadFixedSize) + uint64(len(rec.Command.Symbol)) + uint64(len(rec.Result.Events))*eventEncodedSize
	if payloadLen > uint64(^uint32(0)) {
		return 0, fmt.Errorf("record payload too large")
	}
	return int(payloadLen), nil
}

func encodePayload(buf []byte, rec Record) {
	offset := 0
	putUint8 := func(value uint8) {
		buf[offset] = value
		offset++
	}
	putUint16 := func(value uint16) {
		binary.LittleEndian.PutUint16(buf[offset:offset+2], value)
		offset += 2
	}
	putUint64 := func(value uint64) {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], value)
		offset += 8
	}

	putUint16(uint16(len(rec.Command.Symbol)))
	offset += copy(buf[offset:], rec.Command.Symbol)
	putUint8(uint8(rec.Command.Type))
	putUint64(rec.Command.OrderID)
	putUint8(uint8(rec.Command.Side))
	putUint8(uint8(rec.Command.OrderType))
	putUint8(uint8(rec.Command.TimeInForce))
	putUint64(uint64(rec.Command.Price))
	putUint64(uint64(rec.Command.Quantity))
	putUint64(uint64(rec.Command.NewPrice))
	putUint64(uint64(rec.Command.NewLeavesQty))
	putUint64(rec.Result.CommandSeq)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(rec.Result.Events)))
	offset += 4

	for _, event := range rec.Result.Events {
		putUint8(uint8(event.Type))
		putUint16(uint16(event.Reason))
		putUint64(event.OrderID)
		putUint64(event.MakerOrderID)
		putUint64(event.TakerOrderID)
		putUint64(event.TradeID)
		putUint64(uint64(event.Price))
		putUint64(uint64(event.Quantity))
		putUint64(uint64(event.LeavesQty))
		putUint64(uint64(event.CanceledQty))
		putUint8(uint8(event.CancelReason))
		if event.PriorityRetained {
			putUint8(1)
		} else {
			putUint8(0)
		}
	}
}

func decodePayload(payload []byte, rec *Record) error {
	if len(payload) < 2 {
		return io.ErrUnexpectedEOF
	}
	symbolLen := int(binary.LittleEndian.Uint16(payload[:2]))
	if len(payload) < payloadFixedSize+symbolLen {
		return io.ErrUnexpectedEOF
	}
	offset := 2
	readUint8 := func() uint8 {
		value := payload[offset]
		offset++
		return value
	}
	readUint16 := func() uint16 {
		value := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		return value
	}
	readUint64 := func() uint64 {
		value := binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
		return value
	}

	rec.Command.Symbol = string(payload[offset : offset+symbolLen])
	offset += symbolLen
	rec.Command.Type = model.CommandType(readUint8())
	rec.Command.OrderID = readUint64()
	rec.Command.Side = model.Side(readUint8())
	rec.Command.OrderType = model.OrderType(readUint8())
	rec.Command.TimeInForce = model.TimeInForce(readUint8())
	rec.Command.Price = int64(readUint64())
	rec.Command.Quantity = int64(readUint64())
	rec.Command.NewPrice = int64(readUint64())
	rec.Command.NewLeavesQty = int64(readUint64())
	rec.Result.CommandSeq = readUint64()
	eventCount := binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4

	eventsSize := uint64(eventCount) * eventEncodedSize
	remaining := uint64(len(payload) - offset)
	if eventsSize > remaining {
		return io.ErrUnexpectedEOF
	}
	if eventsSize < remaining {
		return fmt.Errorf("unexpected payload tail: %d", remaining-eventsSize)
	}

	rec.Result.Events = make([]model.Event, int(eventCount))
	for i := range rec.Result.Events {
		event := &rec.Result.Events[i]
		event.Type = model.EventType(readUint8())
		event.Reason = model.RejectReason(readUint16())
		event.OrderID = readUint64()
		event.MakerOrderID = readUint64()
		event.TakerOrderID = readUint64()
		event.TradeID = readUint64()
		event.Price = int64(readUint64())
		event.Quantity = int64(readUint64())
		event.LeavesQty = int64(readUint64())
		event.CanceledQty = int64(readUint64())
		event.CancelReason = model.CancelReason(readUint8())
		event.PriorityRetained = readUint8() == 1
	}

	rec.Command.Seq = rec.CommandSeq
	return nil
}
