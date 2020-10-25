package lkleader

import (
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLeader_RequestVoteRequest(t *testing.T) {
	lead := newLeader(t)
	lead.Nodes = []string{}

	tests := []struct {
		name         string
		startState   int
		endState     int
		useNodes     bool
		nodeResponse requestVoteResponse
		startTerm    int
		endTerm      int
		err          error
	}{
		{
			name:       "00 already leader",
			startState: StateLeader,
			endState:   StateLeader,
			useNodes:   false,
			nodeResponse: requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: true,
			},
			startTerm: 1,
			endTerm:   1,
			err:       nil,
		},
		{
			name:       "01 no peers this state is leader",
			startState: StateLeader,
			endState:   StateLeader,
			useNodes:   false,
			nodeResponse: requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: true,
			},
			startTerm: 1,
			endTerm:   1,
			err:       nil,
		},
		{
			name:       "02 vote granted",
			startState: StateCandidate,
			endState:   StateCandidate,
			useNodes:   true,
			nodeResponse: requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: true,
			},
			startTerm: 1,
			endTerm:   1,
			err:       nil,
		},
		{
			name:       "03 deny vote",
			startState: StateFollower,
			endState:   StateFollower,
			useNodes:   true,
			nodeResponse: requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: false,
			},
			startTerm: 1,
			endTerm:   1,
			err:       ErrTooFewVotes,
		},
		{
			name:       "04 step down",
			startState: StateFollower,
			endState:   StateFollower,
			useNodes:   true,
			nodeResponse: requestVoteResponse{
				Term:        lead.State.Term() + 2,
				VoteGranted: true,
			},
			startTerm: 1,
			endTerm:   3,
			err:       ErrNewElectionTerm,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeResponse := tt.nodeResponse
			lead.State.Term(tt.startTerm)
			lead.State.State(tt.startState)
			if tt.useNodes {
				node0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node0.Close()
				node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node1.Close()
				node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node2.Close()
				node3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node3.Close()
				node4 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node4.Close()
				lead.Nodes = []string{node0.URL, node1.URL, node2.URL, node3.URL, node4.URL}
			} else {
				node0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node0.Close()
				lead.Nodes = []string{node0.URL}
			}
			currentTerm, err := lead.RequestVoteRequest()
			equals(t, tt.err, err)
			equals(t, lead.State.StateString(tt.endState), lead.State.StateString(lead.State.State()))
			equals(t, tt.endTerm, currentTerm)
		})
	}
}

func TestLeader_HeartBeatRequest(t *testing.T) {
	lead := newLeader(t)

	tests := []struct {
		name         string
		startState   int
		endState     int
		useNodes     bool
		nodeResponse heartBeatResponse
		startTerm    int
		endTerm      int
		err          error
	}{
		{
			name:       "00 follower",
			startState: StateFollower,
			endState:   StateFollower,
			useNodes:   true,
			nodeResponse: heartBeatResponse{
				Term:    1,
				Success: true,
			},
			startTerm: 1,
			endTerm:   1,
			err:       nil,
		},
		{
			name:       "01 step down",
			startState: StateFollower,
			endState:   StateFollower,
			useNodes:   true,
			nodeResponse: heartBeatResponse{
				Term:    2,
				Success: true,
			},
			startTerm: 1,
			endTerm:   2,
			err:       ErrNewElectionTerm,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeResponse := tt.nodeResponse
			lead.State.Term(tt.startTerm)
			lead.State.State(tt.startState)
			if tt.useNodes {
				node0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node0.Close()
				node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node1.Close()
				node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node2.Close()
				node3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node3.Close()
				node4 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gob.NewEncoder(w).Encode(nodeResponse)
				}))
				defer node4.Close()
				lead.Nodes = []string{node0.URL, node1.URL, node2.URL, node3.URL, node4.URL}
			}
			currentTerm, err := lead.HeartBeatRequest()
			equals(t, tt.err, err)
			equals(t, lead.State.StateString(tt.endState), lead.State.StateString(lead.State.State()))
			equals(t, tt.endTerm, currentTerm)
		})
	}
}
