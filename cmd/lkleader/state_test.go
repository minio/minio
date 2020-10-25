package lkleader

import "testing"

func TestNewState(t *testing.T) {
	type args struct {
		id int
	}
	tests := []struct {
		name   string
		args   args
		wantID int
	}{
		{"00", args{1}, 1},
		{"00", args{123}, 123},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewState(tt.args.id, 0, 0)
			equals(t, tt.wantID, got.ID())
			equals(t, 1, got.Term())
		})
	}
}

func TestState_ID(t *testing.T) {
	tests := []struct {
		name  string
		state *State
		want  int
	}{
		{"00", NewState(1, 0, 0), 1},
		{"01", NewState(123, 0, 0), 123},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equals(t, tt.want, tt.state.ID())
		})
	}
}

func TestState_LeaderID(t *testing.T) {
	s := NewState(1, 0, 0)
	equals(t, UnknownLeaderID, s.LeaderID())
	s.LeaderID(33)
	equals(t, 33, s.LeaderID())
}

func TestState_State(t *testing.T) {
	s := NewState(1, 0, 0)
	equals(t, StateFollower, s.State())
	s.State(StateCandidate)
	equals(t, StateCandidate, s.State())
	s.State(StateLeader)
	equals(t, StateLeader, s.State())
}

func TestState_Term(t *testing.T) {
	s := NewState(1, 0, 0)
	equals(t, 1, s.Term())
	s.Term(s.Term() + 1)
	equals(t, 2, s.Term())
}

func TestState_VotedFor(t *testing.T) {
	s := NewState(1, 0, 0)
	equals(t, NoVote, s.VotedFor())
	s.VotedFor(33)
	equals(t, 33, s.VotedFor())
}

func TestState_StepDown(t *testing.T) {
	s := NewState(1, 0, 0)
	s.State(StateLeader)
	s.VotedFor(33)
	s.StepDown(5)
	equals(t, 5, s.Term())
	equals(t, StateFollower, s.State())
	equals(t, NoVote, s.VotedFor())
}

func TestState_StateString(t *testing.T) {
	s := NewState(1, 0, 0)
	equals(t, "candidate", s.StateString(StateCandidate))
	equals(t, "follower", s.StateString(StateFollower))
	equals(t, "leader", s.StateString(StateLeader))
}
