package versioning

import (
	"encoding/xml"
	"io"
)

// State - enabled/disabled/suspended states
// for multifactor and status of versioning.
type State string

// Various supported states
const (
	Enabled   State = "Enabled"
	Disabled  State = "Disabled"
	Suspended State = "Suspended"
)

// Versioning - Configuration for bucket versioning.
type Versioning struct {
	XMLName   xml.Name `xml:"VersioningConfiguration"`
	MFADelete State    `xml:"MFADelete,omitempty"`
	Status    State    `xml:"Status,omitempty"`
}

// Validate - validates the versioning configuration
func (v Versioning) Validate() error {
	switch v.MFADelete {
	case Enabled, Disabled, "":
	default:
		return Errorf("unsupported MFADelete state %s", v.MFADelete)
	}
	switch v.Status {
	case Enabled, Suspended, "":
	default:
		return Errorf("unsupported Versioning status %s", v.Status)
	}
	return nil
}

// ParseConfig - parses data in given reader to VersioningConfiguration.
func ParseConfig(reader io.Reader) (*Versioning, error) {
	var v Versioning
	if err := xml.NewDecoder(reader).Decode(&v); err != nil {
		return nil, err
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return &v, nil
}
