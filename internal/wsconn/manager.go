package wsconn

import (
	"sync"

	"github.com/google/uuid"
)

// Manager will
type Manager struct {
	ID uuid.UUID

	// string -> *Connection
	// We expect few writes after startup
	targets sync.Map
}
