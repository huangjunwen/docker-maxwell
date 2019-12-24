package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamId(t *testing.T) {
	assert := assert.New(t)

	s := "mysql-bin.002522:137596"
	pos := ParseMaxwellBinlogPos(s)
	id := StreamId{
		BinlogPos: pos,
		Counter:   11,
	}

	id2 := ParseStreamId(id.String())
	assert.True(id2.Valid())
	assert.Equal(id, id2)

}
