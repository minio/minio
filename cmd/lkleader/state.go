package lkleader

import (
	"encoding/gob"
	"encoding/json"
	"io"
	"sync"
	"time"
)

const (
	// UnknownLeaderID is set when a new election is in progress.
	UnknownLeaderID = 0
	// NoVote is set to represent the node has not voted.
	NoVote = 0
	// StateCandidate represents the raft candidate state
	StateCandidate = iota
	// StateFollower represents the raft follower state
	StateFollower
	// StateLeader represents the raft leader state
	StateLeader
)

var (
	// DefaultElectionTickRange will set the range of numbers for the election timeout. For example
	// a value of 1500 will first hash the input Addr to a number from 0 to 1500 and then
	// add that 1500 to give a result between 1500 and 3000
	DefaultElectionTickRange = 4000
	// DefaultHeartbeatTickRange will set the range of numbers for the heartbeat timeout.
	DefaultHeartbeatTickRange = 2000
)

// Status is used to show the current states status.
type Status struct {
	ID       int
	LeaderID int
	State    string
	Term     int
	VotedFor int
}

// State encapsulates the current nodes raft state.
type State struct {
	// heartBeatChan holds events for HeartBeat, it will use the term value
	heartBeatChan chan *HeartBeatRequest
	// id is this nodes id, it will be set to the hashed addr % 1000
	id int
	// heartbeatResetChan is used to reset a heartbeat ticker.
	heartbeatResetChan chan struct{}
	// leaderID is the current leader.
	leaderID int
	mu       *sync.Mutex
	// state is one of Follower, Candidate, Leader
	state int
	// stateChangeChan informs any listener of a state change and
	// the state id that was changed.
	stateChangeChan chan int
	// term is the current term for consensus.
	term int
	// votedFor is used during election.
	votedFor int

	// electionTimeout specifies the time in the candidate state without
	// a leader before we attempt an election.
	electionTimeoutMS int
	// heartbeatTimeoutMS specifies the time in follower state without
	// a leader before we attempt an election. Typically 100ms to 500ms
	heartbeatTimeoutMS int
}

// NewState initializes a new raft state.
func NewState(id int, electionTimeoutMS, heartbeatTimeoutMS int) *State {
	return &State{
		heartBeatChan:      make(chan *HeartBeatRequest),
		electionTimeoutMS:  electionTimeoutMS,
		heartbeatResetChan: make(chan struct{}),
		heartbeatTimeoutMS: heartbeatTimeoutMS,
		id:                 id,
		leaderID:           UnknownLeaderID,
		mu:                 &sync.Mutex{},
		state:              StateFollower,
		stateChangeChan:    make(chan int),
		term:               1,
		votedFor:           NoVote,
	}
}

// String will return the current states fields for debugging.
func (s *State) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, _ := json.Marshal(Status{
		ID:       s.id,
		LeaderID: s.leaderID,
		State:    s.StateString(s.state),
		Term:     s.term,
		VotedFor: s.votedFor,
	})
	return string(d)
}

// WriteTo will return the current states fields for debugging.
func (s *State) WriteTo(w io.Writer) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := gob.NewEncoder(w).Encode(Status{
		ID:       s.id,
		LeaderID: s.leaderID,
		State:    s.StateString(s.state),
		Term:     s.term,
		VotedFor: s.votedFor,
	})
	return 0, err
}

// ID returns the nodes id.
func (s *State) ID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// LeaderID will return the states current leader id or if
// an argument is passed in will set the current LeaderID.
func (s *State) LeaderID(id ...int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(id) > 0 {
		s.leaderID = id[0]
	}
	return s.leaderID
}

// StateString returns the current state as a string.
func (s *State) StateString(state int) string {
	switch state {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return "unknown"
}

// State will return the states current state or if
// an argument is passed in will set the state
func (s *State) State(state ...int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(state) > 0 {
		if s.state != state[0] {
			s.state = state[0]
			go func() {
				s.stateChangeChan <- state[0]
			}()
		}
	}
	return s.state
}

// Term will return the states current term or if
// an argument is passed in will set the term
func (s *State) Term(term ...int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(term) > 0 {
		s.term = term[0]
	}
	return s.term
}

// VotedFor will return the states current vote or if
// an argument is passed in will set the vote
func (s *State) VotedFor(votedFor ...int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(votedFor) > 0 {
		s.votedFor = votedFor[0]
	}
	return s.votedFor
}

// StepDown will step down the state by resetting to the given term
// and emitting a state change.
func (s *State) StepDown(term int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = NoVote
	if s.state != StateFollower {
		s.state = StateFollower
		go func() {
			s.stateChangeChan <- StateFollower
		}()
	}
}

// StateChanged returns a channel for any state changes that occur.
func (s *State) StateChanged() chan int {
	return s.stateChangeChan
}

// HeartBeatEvent returns a channel for any succesful append entries events.
func (s *State) HeartBeatEvent(event ...*HeartBeatRequest) chan *HeartBeatRequest {
	if len(event) > 0 {
		go func() {
			s.heartBeatChan <- event[0]
		}()
	}
	return s.heartBeatChan
}

// ElectionTick returns a channel with a new random election tick.
func (s *State) ElectionTick() <-chan time.Time {
	return randomTimeout(time.Duration(s.electionTimeoutMS) * time.Millisecond)
}

// HeartbeatReset will signal a reset. This works with a listener for HeartbeatTick.
func (s *State) HeartbeatReset(reset ...bool) <-chan struct{} {
	if len(reset) > 0 {
		s.heartbeatResetChan <- struct{}{}
	}
	return s.heartbeatResetChan
}

// HeartbeatTick returns a channel with a heartbeat timeout set to heartbeatTimeoutMS.
func (s *State) HeartbeatTick() <-chan time.Time {
	return time.After(time.Duration(s.heartbeatTimeoutMS) * time.Millisecond)
}

// HeartbeatTickRandom returns a channel with a random heartbeat timeout.
// 500ms is added to the minimum heartbeatTimeoutMS to compensate for possible network latency.
func (s *State) HeartbeatTickRandom() <-chan time.Time {
	return randomTimeout(time.Duration(s.heartbeatTimeoutMS+500) * time.Millisecond)
}
