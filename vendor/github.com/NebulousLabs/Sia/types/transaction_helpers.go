// +build testing

package types

import (
	"errors"
)

// TransactionGraphEdge defines an edge in a TransactionGraph, containing a
// source transaction, a destination transaction, a value, and a miner fee.
type TransactionGraphEdge struct {
	Dest   int
	Fee    Currency
	Source int
	Value  Currency
}

// TransactionGraph will return a set of valid transactions that all spend
// outputs according to the input graph. Each [source, dest] pair defines an
// edge of the graph. The graph must be fully connected and the granparent of
// the graph must be the sourceOutput. '0' refers to an edge from the source
// output. Each edge also specifies a value for the output, and an amount of
// fees. If the fees are zero, no fees will be added for that edge. 'sources'
// must be sorted.
//
// Example of acceptable input:
//
// sourceOutput: // a valid siacoin output spending to UnlockConditions{}.UnlockHash()
//
// Sources: [0, 0, 1, 2, 3, 3, 3, 4]
// Dests:   [1, 2, 3, 3, 4, 4, 5, 6]
//
// Resulting Graph:
//
//    o
//   / \
//  o   o
//   \ /
//    o
//   /|\
//   \| \
//    o  x // 'x' transactions are symbolic, not actually created
//    |
//    x
//
func TransactionGraph(sourceOutput SiacoinOutputID, edges []TransactionGraphEdge) ([]Transaction, error) {
	// Basic input validation.
	if len(edges) < 1 {
		return nil, errors.New("no graph specificed")
	}

	// Check that the first value of 'sources' is zero, and that the rest of the
	// array is sorted.
	if edges[0].Source != 0 {
		return nil, errors.New("first edge must speficy node 0 as the parent")
	}
	if edges[0].Dest != 1 {
		return nil, errors.New("first edge must speficy node 1 as the child")
	}
	latest := edges[0].Source
	for _, edge := range edges {
		if edge.Source < latest {
			return nil, errors.New("'sources' input is not sorted")
		}
		latest = edge.Source
	}

	// Create the set of output ids, and fill out the input ids for the source
	// transaction.
	biggest := 0
	for _, edge := range edges {
		if edge.Dest > biggest {
			biggest = edge.Dest
		}
	}
	txnInputs := make([][]SiacoinOutputID, biggest+1)
	txnInputs[0] = []SiacoinOutputID{sourceOutput}

	// Go through the nodes bit by bit and create outputs.
	// Fill out the outputs for the source.
	i, j := 0, 0
	ts := make([]Transaction, edges[len(edges)-1].Source+1)
	for i < len(edges) {
		var t Transaction

		// Grab the inputs for this transaction.
		for _, outputID := range txnInputs[j] {
			t.SiacoinInputs = append(t.SiacoinInputs, SiacoinInput{
				ParentID: outputID,
			})
		}

		// Grab the outputs for this transaction.
		startingPoint := i
		current := edges[i].Source
		for i < len(edges) && edges[i].Source == current {
			t.SiacoinOutputs = append(t.SiacoinOutputs, SiacoinOutput{
				Value:      edges[i].Value,
				UnlockHash: UnlockConditions{}.UnlockHash(),
			})
			if !edges[i].Fee.IsZero() {
				t.MinerFees = append(t.MinerFees, edges[i].Fee)
			}
			i++
		}

		// Record the inputs for the next transactions.
		for k := startingPoint; k < i; k++ {
			txnInputs[edges[k].Dest] = append(txnInputs[edges[k].Dest], t.SiacoinOutputID(uint64(k-startingPoint)))
		}
		ts[j] = t
		j++
	}

	return ts, nil
}
