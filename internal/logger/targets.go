// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package logger

// Target is the entity that we will receive
// a single log entry and Send it to the log target
//   e.g. Send the log to a http server
type Target interface {
	String() string
	Endpoint() string
	Validate() error
	Send(entry interface{}, errKind string) error
}

// Targets is the set of enabled loggers
var Targets = []Target{}

// AuditTargets is the list of enabled audit loggers
var AuditTargets = []Target{}

// AddAuditTarget adds a new audit logger target to the
// list of enabled loggers
func AddAuditTarget(t Target) error {
	if err := t.Validate(); err != nil {
		return err
	}

	AuditTargets = append(AuditTargets, t)
	return nil
}

// AddTarget adds a new logger target to the
// list of enabled loggers
func AddTarget(t Target) error {
	if err := t.Validate(); err != nil {
		return err
	}
	Targets = append(Targets, t)
	return nil
}
