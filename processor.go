package pgxrepl

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// Processor is a struct that represents
type Processor struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	types     *pgtype.Map
	stream    bool
}

// Process processes the data
func (x *Processor) Process(data []byte) error {
	return nil
}
