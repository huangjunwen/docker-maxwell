package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinlogPos(t *testing.T) {
	assert := assert.New(t)

	s := "mysql-bin.002522:137596"
	pos := ParseMaxwellBinlogPos(s)
	assert.True(pos.Valid())
	assert.Equal(uint32(2522), pos.FileNum)
	assert.Equal(uint32(137596), pos.FilePos)

	pos2 := BinlogPosFromUint64(pos.Uint64())
	assert.Equal(pos, pos2)
	assert.Equal(s, pos2.FormatToMaxwellBinlogPos("mysql-bin"))

}
