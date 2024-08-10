package pgxrepl

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// duration := 10 * time.Second
const duration = 10 * time.Second

// State is a struct that represents the state
type State struct {
	Deadline time.Time
	Position pglogrepl.LSN
}

// Error is a struct that represents the error
type Error struct {
	response *pgproto3.ErrorResponse
}

// Error returns the error
func (x *Error) Error() string {
	return fmt.Sprintf(`code: %v, message: %v, detail: %v, table: %v`,
		x.response.Code, x.response.Message, x.response.Detail, x.response.TableName)
}

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
