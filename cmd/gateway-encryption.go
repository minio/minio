package cmd

const (
	// GW_SSE_S3 is set when SSE-S3 encryption needed on both gateway and backend
	GW_SSE_S3 = "S3"
	// GW_SSE_C is set when SSE-C encryption needed on both gateway and backend
	GW_SSE_C = "C"
	// GW_SSE_KMS is set when SSE-KMS double encryption is needed
	GW_SSE_KMS = "KMS"
)
