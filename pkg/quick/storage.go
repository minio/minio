package quick

import "context"

// Storage is simple storage interface to get and save data.
// The interface helps to decouple the configuration store from Etcd
// The interface is a compatible sub set of ks.Backend (which also provides a Etcd implementation)
type Storage interface {
	// Get get vlaue for given key. Returns nil if key does not exists
	Get(ctx context.Context, key string) ([]byte, error)
	// Save saves given key/valye pair
	Save(ctx context.Context, key string, data []byte) error
	// Returns additional storage information
	Info() string
}
