package common

import "testing"

func TestVolumeIdAugmentIncludesBackendID(t *testing.T) {
	volumeID := VolumeIdAugment("backend-a", "vol-a", "fc", "wwn-123")
	if volumeID != "backend-a|vol-a##fc##wwn-123" {
		t.Fatalf("volume ID = %q, want backend-a|vol-a##fc##wwn-123", volumeID)
	}
}

func TestVolumeIdHelpersSupportBackendAwareIDs(t *testing.T) {
	volumeID := "backend-a|vol-a##fc##wwn-123"

	backendID, err := VolumeIdGetBackendID(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetBackendID returned error: %v", err)
	}
	if backendID != "backend-a" {
		t.Fatalf("backendID = %q, want backend-a", backendID)
	}

	volumeName, err := VolumeIdGetName(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetName returned error: %v", err)
	}
	if volumeName != "vol-a" {
		t.Fatalf("volumeName = %q, want vol-a", volumeName)
	}

	protocol, err := VolumeIdGetStorageProtocol(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetStorageProtocol returned error: %v", err)
	}
	if protocol != "fc" {
		t.Fatalf("protocol = %q, want fc", protocol)
	}

	wwn, err := VolumeIdGetWwn(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetWwn returned error: %v", err)
	}
	if wwn != "wwn-123" {
		t.Fatalf("wwn = %q, want wwn-123", wwn)
	}
}

func TestVolumeIdHelpersSupportLegacyIDs(t *testing.T) {
	volumeID := "vol-a##fc##wwn-123"

	backendID, err := VolumeIdGetBackendID(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetBackendID returned error: %v", err)
	}
	if backendID != "" {
		t.Fatalf("backendID = %q, want empty", backendID)
	}

	volumeName, err := VolumeIdGetName(volumeID)
	if err != nil {
		t.Fatalf("VolumeIdGetName returned error: %v", err)
	}
	if volumeName != "vol-a" {
		t.Fatalf("volumeName = %q, want vol-a", volumeName)
	}
}
