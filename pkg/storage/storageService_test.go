package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAttachWithDiscoveryRetryRetriesTransientNoSASDiskFound(t *testing.T) {
	originalTimeout := attachDiscoveryTimeout
	originalInterval := attachDiscoveryRetryInterval
	attachDiscoveryTimeout = 50 * time.Millisecond
	attachDiscoveryRetryInterval = time.Millisecond
	defer func() {
		attachDiscoveryTimeout = originalTimeout
		attachDiscoveryRetryInterval = originalInterval
	}()

	attempts := 0
	path, err := attachWithDiscoveryRetry(context.Background(), "vol-a", "wwn-123", func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("no SAS disk found")
		}
		return "/dev/dm-1", nil
	})
	if err != nil {
		t.Fatalf("attachWithDiscoveryRetry returned error: %v", err)
	}
	if path != "/dev/dm-1" {
		t.Fatalf("path = %q, want /dev/dm-1", path)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestAttachWithDiscoveryRetryDoesNotRetryUnexpectedErrors(t *testing.T) {
	attempts := 0
	_, err := attachWithDiscoveryRetry(context.Background(), "vol-a", "wwn-123", func() (string, error) {
		attempts++
		return "", errors.New("invalid WWN")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestAttachWithDiscoveryRetryRetriesEmptyPath(t *testing.T) {
	originalTimeout := attachDiscoveryTimeout
	originalInterval := attachDiscoveryRetryInterval
	attachDiscoveryTimeout = 50 * time.Millisecond
	attachDiscoveryRetryInterval = time.Millisecond
	defer func() {
		attachDiscoveryTimeout = originalTimeout
		attachDiscoveryRetryInterval = originalInterval
	}()

	attempts := 0
	path, err := attachWithDiscoveryRetry(context.Background(), "vol-a", "wwn-123", func() (string, error) {
		attempts++
		if attempts < 2 {
			return "", nil
		}
		return "/dev/dm-2", nil
	})
	if err != nil {
		t.Fatalf("attachWithDiscoveryRetry returned error: %v", err)
	}
	if path != "/dev/dm-2" {
		t.Fatalf("path = %q, want /dev/dm-2", path)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}
