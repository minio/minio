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

package event

import (
	"encoding/xml"
	"fmt"
)

// IsEventError - checks whether given error is event error or not.
func IsEventError(err error) bool {
	switch err.(type) {
	case ErrInvalidFilterName, *ErrInvalidFilterName:
		return true
	case ErrFilterNamePrefix, *ErrFilterNamePrefix:
		return true
	case ErrFilterNameSuffix, *ErrFilterNameSuffix:
		return true
	case ErrInvalidFilterValue, *ErrInvalidFilterValue:
		return true
	case ErrDuplicateEventName, *ErrDuplicateEventName:
		return true
	case ErrUnsupportedConfiguration, *ErrUnsupportedConfiguration:
		return true
	case ErrDuplicateQueueConfiguration, *ErrDuplicateQueueConfiguration:
		return true
	case ErrUnknownRegion, *ErrUnknownRegion:
		return true
	case ErrARNNotFound, *ErrARNNotFound:
		return true
	case ErrInvalidARN, *ErrInvalidARN:
		return true
	case ErrInvalidEventName, *ErrInvalidEventName:
		return true
	}

	return false
}

// ErrInvalidFilterName - invalid filter name error.
type ErrInvalidFilterName struct {
	FilterName string
}

func (err ErrInvalidFilterName) Error() string {
	return fmt.Sprintf("invalid filter name '%v'", err.FilterName)
}

// ErrFilterNamePrefix - more than one prefix usage error.
type ErrFilterNamePrefix struct{}

func (err ErrFilterNamePrefix) Error() string {
	return "more than one prefix in filter rule"
}

// ErrFilterNameSuffix - more than one suffix usage error.
type ErrFilterNameSuffix struct{}

func (err ErrFilterNameSuffix) Error() string {
	return "more than one suffix in filter rule"
}

// ErrInvalidFilterValue - invalid filter value error.
type ErrInvalidFilterValue struct {
	FilterValue string
}

func (err ErrInvalidFilterValue) Error() string {
	return fmt.Sprintf("invalid filter value '%v'", err.FilterValue)
}

// ErrDuplicateEventName - duplicate event name error.
type ErrDuplicateEventName struct {
	EventName Name
}

func (err ErrDuplicateEventName) Error() string {
	return fmt.Sprintf("duplicate event name '%v' found", err.EventName)
}

// ErrUnsupportedConfiguration - unsupported configuration error.
type ErrUnsupportedConfiguration struct{}

func (err ErrUnsupportedConfiguration) Error() string {
	return "topic or cloud function configuration is not supported"
}

// ErrDuplicateQueueConfiguration - duplicate queue configuration error.
type ErrDuplicateQueueConfiguration struct {
	Queue Queue
}

func (err ErrDuplicateQueueConfiguration) Error() string {
	var message string
	if data, xerr := xml.Marshal(err.Queue); xerr != nil {
		message = fmt.Sprintf("%+v", err.Queue)
	} else {
		message = string(data)
	}

	return fmt.Sprintf("duplicate queue configuration %v", message)
}

// ErrUnknownRegion - unknown region error.
type ErrUnknownRegion struct {
	Region string
}

func (err ErrUnknownRegion) Error() string {
	return fmt.Sprintf("unknown region '%v'", err.Region)
}

// ErrARNNotFound - ARN not found error.
type ErrARNNotFound struct {
	ARN ARN
}

func (err ErrARNNotFound) Error() string {
	return fmt.Sprintf("ARN '%v' not found", err.ARN)
}

// ErrInvalidARN - invalid ARN error.
type ErrInvalidARN struct {
	ARN string
}

func (err ErrInvalidARN) Error() string {
	return fmt.Sprintf("invalid ARN '%v'", err.ARN)
}

// ErrInvalidEventName - invalid event name error.
type ErrInvalidEventName struct {
	Name string
}

func (err ErrInvalidEventName) Error() string {
	return fmt.Sprintf("invalid event name '%v'", err.Name)
}
