package gojob

import (
	"testing"
	"time"
)

func TestExecutionRetentionDefaults(t *testing.T) {
	if DefaultExecutionSuccessRetention != 15*24*time.Hour {
		t.Fatalf("success retention = %s, want 15 days", DefaultExecutionSuccessRetention)
	}
	if DefaultExecutionOtherRetention != 30*24*time.Hour {
		t.Fatalf("other terminal retention = %s, want 30 days", DefaultExecutionOtherRetention)
	}
	if DefaultRetentionBatchSize != 100 {
		t.Fatalf("retention batch size = %d, want 100", DefaultRetentionBatchSize)
	}
}
