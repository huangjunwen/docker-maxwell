package proxy

import (
	"fmt"
	"strconv"
	"strings"
)

// StreamId represents an id in redis stream.
type StreamId struct {
	BinlogPos

	// Counter is the counter within a same binlog position:
	// NOTE: maxwell may emit several messages in one binlog postion
	Counter uint64
}

// ParseStreamId parse stream id.
func ParseStreamId(s string) StreamId {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return StreamId{}
	}

	part0, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamId{}
	}

	part1, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return StreamId{}
	}

	return StreamId{
		BinlogPos: BinlogPosFromUint64(part0),
		Counter:   part1,
	}
}

// String impelements Stringer interface.
func (id StreamId) String() string {
	// Returns an invalid redis stream id.
	if !id.Valid() {
		return "0-0"
	}
	return fmt.Sprintf("%d-%d", id.BinlogPos.Uint64(), id.Counter)
}

// Format impelements fmt.Formatter interface.
func (id StreamId) Format(f fmt.State, c rune) {
	if c == 's' {
		fmt.Fprint(f, id.String())
		return
	}
	fmt.Fprintf(f, "<StreamId: %d:%d %s>", id.BinlogPos.FileNum, id.BinlogPos.FilePos, id.String())
}

// After returns true if id is larger than another.
func (id StreamId) After(another StreamId) bool {
	u := id.BinlogPos.Uint64()
	if u == 0 {
		return false
	}

	anotherU := another.BinlogPos.Uint64()
	if u > anotherU {
		return true
	}
	if anotherU > u {
		return false
	}
	return id.Counter > another.Counter
}
