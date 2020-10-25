// Package lkleader is a simple leader election mechanism following the
// raft protocol https://raft.github.io/ using http as transport layer
// with internode data format used is `gob`.
//
// This is not a complete raft implementation it is used by MinIO for
// its limited purposes, for a more complete raft implementation and
// advanced features you may use https://github.com/coreos/etcd/raft
// or https://github.com/hashicorp/raft
//
// Leader Election
//
// The process begins with all nodes in 'FOLLOWER' state and waiting for
// for an election timeout. This timeout is recommended chosen to be
// randomized between 150ms to 300ms, in order to reduce the
// internode traffic its also increased to ElectionTickRange.
//
// After the election timeout, the FOLLOWER becomes a CANDIDATE and
// begins an election term by voting for itself and then sends out
// a RequestVote to all other nodes.
//
// If the receiving nodes haven't voted yet, they will then vote for
// the candidate with a successful request. The current node will
// reset it's election timeout and subsequently once the canditate
// has a majority vote it becomes a 'LEADER'
//
// At this point the 'LEADER' will begin sending an HeartBeat request
// to all other nodes specified at a rate specified by hearbeat timeout.
//
// The heartbeat timeout is shorter than the election timeout, by a factor 10
// Followers respond to the HeartBeat, and this term will continue until a
// follower stops receiving a heartbeat and becomes a candidate.
package lkleader
