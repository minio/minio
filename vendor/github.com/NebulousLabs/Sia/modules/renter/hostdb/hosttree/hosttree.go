package hosttree

import (
	"errors"
	"sort"
	"sync"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
	"github.com/NebulousLabs/fastrand"
)

var (
	// errWeightTooHeavy is returned from a SelectRandom() call if a weight that exceeds
	// the total weight of the tree is requested.
	errWeightTooHeavy = errors.New("requested a too-heavy weight")

	// errNegativeWeight is returned from an Insert() call if an entry with a
	// negative weight is added to the tree. Entries must always have a positive
	// weight.
	errNegativeWeight = errors.New("cannot insert using a negative weight")

	// errNilEntry is returned if a fetch call results in a nil tree entry. nodes
	// should always have a non-nil entry, unless they have been Delete()ed.
	errNilEntry = errors.New("node has a nil entry")

	// errHostExists is returned if an Insert is called with a public key that
	// already exists in the tree.
	errHostExists = errors.New("host already exists in the tree")

	// errNoSuchHost is returned if Remove is called with a public key that does
	// not exist in the tree.
	errNoSuchHost = errors.New("no host with specified public key")
)

type (
	// WeightFunc is a function used to weight a given HostDBEntry in the tree.
	WeightFunc func(modules.HostDBEntry) types.Currency

	// HostTree is used to store and select host database entries. Each HostTree
	// is initialized with a weighting func that is able to assign a weight to
	// each entry. The entries can then be selected at random, weighted by the
	// weight func.
	HostTree struct {
		root *node

		// hosts is a map of public keys to nodes.
		hosts map[string]*node

		// weightFn calculates the weight of a hostEntry
		weightFn WeightFunc

		mu sync.Mutex
	}

	// hostEntry is an entry in the host tree.
	hostEntry struct {
		modules.HostDBEntry
		weight types.Currency
	}

	// node is a node in the tree.
	node struct {
		parent *node
		left   *node
		right  *node

		count int  // cumulative count of this node and all children
		taken bool // `taken` indicates whether there is an active host at this node or not.

		weight types.Currency
		entry  *hostEntry
	}
)

// createNode creates a new node using the provided `parent` and `entry`.
func createNode(parent *node, entry *hostEntry) *node {
	return &node{
		parent: parent,
		weight: entry.weight,
		count:  1,

		taken: true,
		entry: entry,
	}
}

// New creates a new, empty, HostTree. It takes one argument, a `WeightFunc`,
// which is used to determine the weight of a node on Insert.
func New(wf WeightFunc) *HostTree {
	return &HostTree{
		root: &node{
			count: 1,
		},
		weightFn: wf,
		hosts:    make(map[string]*node),
	}
}

// recursiveInsert inserts an entry into the appropriate place in the tree. The
// running time of recursiveInsert is log(n) in the maximum number of elements
// that have ever been in the tree.
func (n *node) recursiveInsert(entry *hostEntry) (nodesAdded int, newnode *node) {
	// If there is no parent and no children, and the node is not taken, assign
	// this entry to this node.
	if n.parent == nil && n.left == nil && n.right == nil && !n.taken {
		n.entry = entry
		n.taken = true
		n.weight = entry.weight
		newnode = n
		return
	}

	n.weight = n.weight.Add(entry.weight)

	// If the current node is empty, add the entry but don't increase the
	// count.
	if !n.taken {
		n.taken = true
		n.entry = entry
		newnode = n
		return
	}

	// Insert the element into the lest populated side.
	if n.left == nil {
		n.left = createNode(n, entry)
		nodesAdded = 1
		newnode = n.left
	} else if n.right == nil {
		n.right = createNode(n, entry)
		nodesAdded = 1
		newnode = n.right
	} else if n.left.count <= n.right.count {
		nodesAdded, newnode = n.left.recursiveInsert(entry)
	} else {
		nodesAdded, newnode = n.right.recursiveInsert(entry)
	}

	n.count += nodesAdded
	return
}

// nodeAtWeight grabs an element in the tree that appears at the given weight.
// Though the tree has an arbitrary sorting, a sufficiently random weight will
// pull a random element. The tree is searched through in a post-ordered way.
func (n *node) nodeAtWeight(weight types.Currency) *node {
	// Sanity check - weight must be less than the total weight of the tree.
	if weight.Cmp(n.weight) > 0 {
		build.Critical("Node weight corruption")
		return nil
	}

	// Check if the left or right child should be returned.
	if n.left != nil {
		if weight.Cmp(n.left.weight) < 0 {
			return n.left.nodeAtWeight(weight)
		}
		weight = weight.Sub(n.left.weight) // Search from the 0th index of the right side.
	}
	if n.right != nil && weight.Cmp(n.right.weight) < 0 {
		return n.right.nodeAtWeight(weight)
	}

	// Should we panic here instead?
	if !n.taken {
		build.Critical("Node tree structure corruption")
		return nil
	}

	// Return the root entry.
	return n
}

// remove takes a node and removes it from the tree by climbing through the
// list of parents. remove does not delete nodes.
func (n *node) remove() {
	n.weight = n.weight.Sub(n.entry.weight)
	n.taken = false
	current := n.parent
	for current != nil {
		current.weight = current.weight.Sub(n.entry.weight)
		current = current.parent
	}
}

// All returns all of the hosts in the host tree, sorted by weight.
func (ht *HostTree) All() []modules.HostDBEntry {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	var he []hostEntry
	for _, node := range ht.hosts {
		he = append(he, *node.entry)
	}
	sort.Sort(byWeight(he))

	var entries []modules.HostDBEntry
	for _, entry := range he {
		entries = append(entries, entry.HostDBEntry)
	}
	return entries
}

// Insert inserts the entry provided to `entry` into the host tree. Insert will
// return an error if the input host already exists.
func (ht *HostTree) Insert(hdbe modules.HostDBEntry) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	entry := &hostEntry{
		HostDBEntry: hdbe,
		weight:      ht.weightFn(hdbe),
	}

	if _, exists := ht.hosts[string(entry.PublicKey.Key)]; exists {
		return errHostExists
	}

	_, node := ht.root.recursiveInsert(entry)

	ht.hosts[string(entry.PublicKey.Key)] = node
	return nil
}

// Remove removes the host with the public key provided by `pk`.
func (ht *HostTree) Remove(pk types.SiaPublicKey) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	node, exists := ht.hosts[string(pk.Key)]
	if !exists {
		return errNoSuchHost
	}
	node.remove()
	delete(ht.hosts, string(pk.Key))

	return nil
}

// Modify updates a host entry at the given public key, replacing the old entry
// with the entry provided by `newEntry`.
func (ht *HostTree) Modify(hdbe modules.HostDBEntry) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	node, exists := ht.hosts[string(hdbe.PublicKey.Key)]
	if !exists {
		return errNoSuchHost
	}

	node.remove()

	entry := &hostEntry{
		HostDBEntry: hdbe,
		weight:      ht.weightFn(hdbe),
	}

	_, node = ht.root.recursiveInsert(entry)

	ht.hosts[string(entry.PublicKey.Key)] = node
	return nil
}

// Select returns the host with the provided public key, should the host exist.
func (ht *HostTree) Select(spk types.SiaPublicKey) (modules.HostDBEntry, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	node, exists := ht.hosts[string(spk.Key)]
	if !exists {
		return modules.HostDBEntry{}, false
	}
	return node.entry.HostDBEntry, true
}

// SelectRandom grabs a random n hosts from the tree. There will be no repeats, but
// the length of the slice returned may be less than n, and may even be zero.
// The hosts that are returned first have the higher priority. Hosts passed to
// 'ignore' will not be considered; pass `nil` if no blacklist is desired.
func (ht *HostTree) SelectRandom(n int, ignore []types.SiaPublicKey) []modules.HostDBEntry {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	var hosts []modules.HostDBEntry
	var removedEntries []*hostEntry

	for _, pubkey := range ignore {
		node, exists := ht.hosts[string(pubkey.Key)]
		if !exists {
			continue
		}
		node.remove()
		delete(ht.hosts, string(pubkey.Key))
		removedEntries = append(removedEntries, node.entry)
	}

	for len(hosts) < n && len(ht.hosts) > 0 {
		randWeight := fastrand.BigIntn(ht.root.weight.Big())
		node := ht.root.nodeAtWeight(types.NewCurrency(randWeight))

		if node.entry.AcceptingContracts &&
			len(node.entry.ScanHistory) > 0 &&
			node.entry.ScanHistory[len(node.entry.ScanHistory)-1].Success {
			// The host must be online and accepting contracts to be returned
			// by the random function.
			hosts = append(hosts, node.entry.HostDBEntry)
		}

		removedEntries = append(removedEntries, node.entry)
		node.remove()
		delete(ht.hosts, string(node.entry.PublicKey.Key))
	}

	for _, entry := range removedEntries {
		_, node := ht.root.recursiveInsert(entry)
		ht.hosts[string(entry.PublicKey.Key)] = node
	}

	return hosts
}
