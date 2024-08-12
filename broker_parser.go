package pgxrepl

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// Parser is a struct that represents
type Parser struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	types     *pgtype.Map
	stream    bool
}

// Parse processes the data
func (x *Parser) Parse(data []byte) error {
	return nil
}
