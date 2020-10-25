package lkleader

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestLeader_StatusHandler(t *testing.T) {
	lead := newLeader(t)

	router := lead.Router()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, lead.routePrefix+RouteStatus, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusMethodNotAllowed, w.Code)

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, lead.routePrefix+RouteStatus, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusOK, w.Code)

	want := Status{
		ID:       1613,
		LeaderID: 0,
		State:    "follower",
		Term:     1,
		VotedFor: 0,
	}
	var got Status
	gob.NewDecoder(w.Body).Decode(&got)
	equals(t, want, got)
}

func TestLeader_StepDownHandler(t *testing.T) {
	lead := newLeader(t)
	lead.State.State(StateLeader)
	router := lead.Router()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, lead.routePrefix+RouteStepDown, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusMethodNotAllowed, w.Code)

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, lead.routePrefix+RouteStepDown, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusOK, w.Code)

	want := Status{
		ID:       1613,
		LeaderID: 0,
		State:    "follower",
		Term:     1,
		VotedFor: 0,
	}
	var got Status
	gob.NewDecoder(w.Body).Decode(&got)
	equals(t, want, got)
}

func TestLeader_IDHandler(t *testing.T) {
	lead := newLeader(t)
	router := lead.Router()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, lead.routePrefix+RouteID, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusMethodNotAllowed, w.Code)

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, lead.routePrefix+RouteID, nil)
	router.ServeHTTP(w, req)
	equals(t, http.StatusOK, w.Code)
	equals(t, strconv.Itoa(lead.State.ID())+"\n", w.Body.String())
}

func TestLeader_RequestVoteHandler(t *testing.T) {
	lead := newLeader(t)
	router := lead.Router()

	tests := []struct {
		name          string
		method        string
		req           *requestVoteRequest
		resp          *requestVoteResponse
		statusCode    int
		startState    int
		endState      int
		startTerm     int
		endTerm       int
		startVotedFor int
		endVotedFor   int
		startLeaderID int
		endLeaderID   int
	}{
		{
			name:          "00 method not allowed",
			method:        http.MethodGet,
			req:           nil,
			resp:          nil,
			statusCode:    http.StatusMethodNotAllowed,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
		{
			name:   "01 ok",
			method: http.MethodPost,
			req: &requestVoteRequest{
				Term:        lead.State.Term(),
				CandidateID: lead.State.ID(),
			},
			resp: &requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: true,
			},
			statusCode:    http.StatusOK,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   lead.State.ID(),
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
		{
			name:   "02 request from an old term so rejected",
			method: http.MethodPost,
			req: &requestVoteRequest{
				Term:        lead.State.Term() - 1,
				CandidateID: lead.State.ID(),
			},
			resp: &requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: false,
				Reason:      "term 0 < 1",
			},
			statusCode:    http.StatusOK,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
		{
			name:   "03 request from an old term so rejected already leader",
			method: http.MethodPost,
			req: &requestVoteRequest{
				Term:        lead.State.Term() - 1,
				CandidateID: lead.State.ID(),
			},
			resp: &requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: false,
				Reason:      "already leader",
			},
			statusCode:    http.StatusOK,
			startState:    StateLeader,
			endState:      StateLeader,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: lead.State.ID(),
			endLeaderID:   lead.State.ID(),
		},
		{
			name:   "04 double vote",
			method: http.MethodPost,
			req: &requestVoteRequest{
				Term:        lead.State.Term(),
				CandidateID: lead.State.ID() + 1,
			},
			resp: &requestVoteResponse{
				Term:        lead.State.Term(),
				VoteGranted: false,
				Reason:      fmt.Sprintf("already cast vote for %d", lead.State.ID()),
			},
			statusCode:    http.StatusOK,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: lead.State.ID(),
			endVotedFor:   lead.State.ID(),
			startLeaderID: lead.State.ID(),
			endLeaderID:   lead.State.ID(),
		},
		{
			name:   "04 newer term",
			method: http.MethodPost,
			req: &requestVoteRequest{
				Term:        lead.State.Term() + 1,
				CandidateID: lead.State.ID(),
			},
			resp: &requestVoteResponse{
				Term:        2,
				VoteGranted: true,
			},
			statusCode:    http.StatusOK,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       2,
			startVotedFor: lead.State.ID(),
			endVotedFor:   lead.State.ID(),
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lead.State.State(tt.startState)
			lead.State.Term(tt.startTerm)
			lead.State.VotedFor(tt.startVotedFor)
			lead.State.LeaderID(tt.startLeaderID)
			w := httptest.NewRecorder()
			var req *http.Request
			var body bytes.Buffer
			if tt.req != nil {
				gob.NewEncoder(&body).Encode(tt.req)
				req = httptest.NewRequest(tt.method, lead.routePrefix+RouteRequestVote, &body)
			} else {
				req = httptest.NewRequest(tt.method, lead.routePrefix+RouteRequestVote, nil)
			}
			router.ServeHTTP(w, req)
			equals(t, tt.statusCode, w.Code)
			if tt.resp != nil {
				var want bytes.Buffer
				gob.NewEncoder(&want).Encode(tt.resp)
				equals(t, want.Bytes(), w.Body.Bytes())
			}
			equals(t, lead.State.StateString(tt.endState), lead.State.StateString(lead.State.State()))
			equals(t, tt.endTerm, lead.State.Term())
			equals(t, tt.endVotedFor, lead.State.VotedFor())
			equals(t, tt.endLeaderID, lead.State.LeaderID())
		})
	}
}

func TestLeader_HeartBeatHandler(t *testing.T) {
	lead := newLeader(t)
	router := lead.Router()

	tests := []struct {
		name          string
		method        string
		req           *HeartBeatRequest
		wantResp      *heartBeatResponse
		statusCode    int
		startState    int
		endState      int
		startTerm     int
		endTerm       int
		startVotedFor int
		endVotedFor   int
		startLeaderID int
		endLeaderID   int
	}{
		{
			name:          "00 method not allowed",
			method:        http.MethodGet,
			req:           nil,
			wantResp:      nil,
			statusCode:    http.StatusMethodNotAllowed,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
		{
			name:   "01 request from an old term so rejected",
			method: http.MethodPost,
			req: &HeartBeatRequest{
				Term:     lead.State.Term() - 1,
				LeaderID: lead.State.ID(),
			},
			wantResp: &heartBeatResponse{
				Term:    lead.State.Term(),
				Success: false,
				Reason:  "term 0 < 1",
			},
			statusCode:    http.StatusOK,
			startState:    StateFollower,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       1,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   UnknownLeaderID,
		},
		{
			name:   "02 request from a newer term so step down",
			method: http.MethodPost,
			req: &HeartBeatRequest{
				Term:     lead.State.Term() + 1,
				LeaderID: 999,
			},
			wantResp: &heartBeatResponse{
				Term:    2,
				Success: true,
			},
			statusCode:    http.StatusOK,
			startState:    StateCandidate,
			endState:      StateFollower,
			startTerm:     1,
			endTerm:       2,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   999,
		},
		{
			name:   "03 request from a equal term so step down",
			method: http.MethodPost,
			req: &HeartBeatRequest{
				Term:     2,
				LeaderID: 999,
			},
			wantResp: &heartBeatResponse{
				Term:    2,
				Success: true,
			},
			statusCode:    http.StatusOK,
			startState:    StateLeader,
			endState:      StateFollower,
			startTerm:     2,
			endTerm:       2,
			startVotedFor: NoVote,
			endVotedFor:   NoVote,
			startLeaderID: UnknownLeaderID,
			endLeaderID:   999,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lead.State.State(tt.startState)
			lead.State.Term(tt.startTerm)
			lead.State.VotedFor(tt.startVotedFor)
			lead.State.LeaderID(tt.startLeaderID)
			w := httptest.NewRecorder()
			var req *http.Request
			var body bytes.Buffer
			if tt.req != nil {
				gob.NewEncoder(&body).Encode(tt.req)
				req = httptest.NewRequest(tt.method, lead.routePrefix+RouteHeartBeat, &body)
			} else {
				req = httptest.NewRequest(tt.method, lead.routePrefix+RouteHeartBeat, nil)
			}
			router.ServeHTTP(w, req)
			equals(t, tt.statusCode, w.Code)
			if tt.wantResp != nil {
				var want bytes.Buffer
				gob.NewEncoder(&want).Encode(tt.wantResp)
				equals(t, want.Bytes(), w.Body.Bytes())
			}
			equals(t, lead.State.StateString(tt.endState), lead.State.StateString(lead.State.State()))
			equals(t, tt.endTerm, lead.State.Term())
			equals(t, tt.endVotedFor, lead.State.VotedFor())
			equals(t, tt.endLeaderID, lead.State.LeaderID())
		})
	}
}
