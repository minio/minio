// Copyright (c) 2015-2022 MinIO, Inc.
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

package loggertypes

// TargetType indicates type of the target e.g. console, http, kafka
type TargetType uint8

//go:generate stringer -type=TargetType -trimprefix=Target $GOFILE

// Constants for target types
const (
	_ TargetType = iota
	TargetConsole
	TargetHTTP
	TargetKafka
)

// TargetStats contains statistics for a target.
type TargetStats struct {
	// QueueLength is the queue length if any messages are queued.
	QueueLength int

	// TotalMessages is the total number of messages sent in the lifetime of the target
	TotalMessages int64

	// FailedMessages should log message count that failed to send.
	FailedMessages int64
}
