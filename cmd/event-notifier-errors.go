package cmd

type EventNotification struct {
	APIErrorResponse
	EventName string `xml:"Event"`
}

func eEventNotification(eName string) error {
	e := EventNotification{}
	e.EventName = eName
	e.ErrCode = ErrEventNotification
	e.Message = "A specified event is not supported for notifications."
	return e
}

type ARNNotification struct {
	APIErrorResponse
	ARN string
}

func eARNNotification(arn string) error {
	e := ARNNotification{}
	e.ARN = arn
	e.ErrCode = ErrARNNotification
	e.Message = "A specified destination ARN does not exist or is not well-formed. Verify the destination ARN."
	return e
}

type RegionNotification struct {
	APIErrorResponse
	Region string
}

func eRegionNotification(region string) error {
	e := RegionNotification{}
	e.Region = region
	e.ErrCode = ErrRegionNotification
	e.Message = "A specified destination is in a different region than the bucket. You must use a destination that resides in the same region as the bucket."
	return e
}

type OverlappingFilterNotification struct {
	APIErrorResponse
}

func eOverlappingFilterNotification() error {
	e := OverlappingFilterNotification{}
	e.ErrCode = ErrOverlappingFilterNotification
	e.Message = "An object key name filtering rule defined with overlapping prefixes, overlapping suffixes, or overlapping combinations of prefixes and suffixes for the same event types."
	return e
}

type FilterValueInvalid struct {
	APIErrorResponse
	FilterValue string
}

func eFilterValueInvalid(value string) error {
	e := FilterValueInvalid{}
	e.FilterValue = value
	e.ErrCode = ErrFilterValueInvalid
	e.Message = "Size of filter rule value cannot exceed 1024 bytes in UTF-8 representation"
	return e
}

type FilterNameInvalid struct {
	APIErrorResponse
}

func eFilterNameInvalid() error {
	e := FilterNameInvalid{}
	e.ErrCode = ErrFilterNameInvalid
	e.Message = "filter rule name must be either prefix or suffix"
	return e
}

type FilterNamePrefix struct {
	APIErrorResponse
}

func eFilterNamePrefix() error {
	e := FilterNamePrefix{}
	e.ErrCode = ErrFilterNamePrefix
	e.Message = "Cannot specify more than one prefix rule in a filter."
	return e
}

type FilterNameSuffix struct {
	APIErrorResponse
}

func eFilterNameSuffix() error {
	e := FilterNameSuffix{}
	e.ErrCode = ErrFilterNameSuffix
	e.Message = "Cannot specify more than one suffix rule in a filter."
	return e
}

type OverlappingConfigs struct {
	APIErrorResponse
}

func eOverlappingConfigs() error {
	e := OverlappingConfigs{}
	e.ErrCode = ErrOverlappingConfigs
	e.Message = "Configurations overlap. Configurations on the same bucket cannot share a common event type."
	return e
}
