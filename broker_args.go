package pgxrepl

// Handler is an interface that represents the handler
type Handler interface {
	Handle(any) error
}

// InsertHandler is a struct that represents the insert handler
type InsertEventArgs struct {
	// NewRow is a map that represents the row
	NewRow map[string]any
	// Table is a string that represents the table
	Table string
}

// UpdateHandler is a struct that represents the update handler
type UpdateEventArgs struct {
	// NewRow is a map that represents the new row
	NewRow map[string]any
	// OldRow is a map that represents the old row
	OldRow map[string]any
	// Table is a string that represents the table
	Table string
}

// DeleteHandler is a struct that represents the delete handler
type DeleteEventArgs struct {
	// OldRow is a map that represents the old row
	OldRow map[string]any
	// Table is a string that represents the table
	Table string
}
