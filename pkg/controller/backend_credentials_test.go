package controller

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	storageapitypes "github.com/Seagate/seagate-exos-x-api-go/v2/pkg/common"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseSnapshotID(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		wantBackendID  string
		wantSnapshotID string
		wantErr        bool
	}{
		{name: "encoded snapshot ID", id: "backend-a|snap-001", wantBackendID: "backend-a", wantSnapshotID: "snap-001"},
		{name: "invalid backend ID", id: "https://array|snap-001", wantErr: true},
		{name: "too many delimiters", id: "a|b|c", wantErr: true},
		{name: "missing backend part", id: "|foo", wantErr: true},
		{name: "missing snapshot part", id: "backend-a|", wantErr: true},
		{name: "empty snapshot ID", id: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backendID, snapshotID, err := parseSnapshotID(tt.id)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.id)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if backendID != tt.wantBackendID || snapshotID != tt.wantSnapshotID {
				t.Fatalf("got backendID=%q snapshotID=%q, want backendID=%q snapshotID=%q", backendID, snapshotID, tt.wantBackendID, tt.wantSnapshotID)
			}
		})
	}
}

func TestLoadBackendConfigsFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "backends.json")
	payload := `{
		"backend-a":{"apiAddress":"https://array-a","username":"user-a","password":"pass-a"},
		"backend-b":{"apiAddress":"https://array-b","apiAddressB":"https://array-b-secondary","username":"user-b","password":"pass-b"}
	}`
	if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	configs, err := loadBackendConfigsFromFile(path)
	if err != nil {
		t.Fatalf("loadBackendConfigsFromFile returned error: %v", err)
	}

	if len(configs) != 2 {
		t.Fatalf("expected 2 backend configs, got %d", len(configs))
	}
	if configs["backend-b"].APIAddressB != "https://array-b-secondary" {
		t.Fatalf("expected secondary address to be loaded")
	}
}

func TestResolveCredentialsForSnapshotID(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
			"backend-b": {APIAddress: "https://array-b", Username: "user-b", Password: "pass-b"},
		},
	}

	backendID, snapshotID, credentials, err := controller.resolveCredentialsForSnapshotID("backend-b|snap-002")
	if err != nil {
		t.Fatalf("resolveCredentialsForSnapshotID returned error: %v", err)
	}

	if backendID != "backend-b" || snapshotID != "snap-002" {
		t.Fatalf("got backendID=%q snapshotID=%q", backendID, snapshotID)
	}
	if credentials["username"] != "user-b" || credentials["password"] != "pass-b" {
		t.Fatalf("resolved wrong credentials for backend-b")
	}
}

func TestResolveCreateSnapshotBackendIDRequiresExplicitBackendIDInMultiBackendMode(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
			"backend-b": {APIAddress: "https://array-b", Username: "user-b", Password: "pass-b"},
		},
	}

	if _, err := controller.resolveCreateSnapshotBackendID(nil); err == nil {
		t.Fatal("expected backendID resolution error")
	}
}

func TestResolveCreateSnapshotBackendIDUsesSingleConfiguredBackend(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
		},
	}

	backendID, err := controller.resolveCreateSnapshotBackendID(map[string]string{})
	if err != nil {
		t.Fatalf("resolveCreateSnapshotBackendID returned error: %v", err)
	}
	if backendID != "backend-a" {
		t.Fatalf("backendID = %q, want backend-a", backendID)
	}
}

func TestListSnapshotsResolvesBackendCredentialsWithoutRequestSecrets(t *testing.T) {
	var configuredCredentials []map[string]string
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
			"backend-b": {APIAddress: "https://array-b", Username: "user-b", Password: "pass-b"},
		},
	}
	controller.configureClientFn = func(credentials map[string]string) error {
		configuredCredentials = append(configuredCredentials, credentials)
		return nil
	}
	controller.showSnapshotsFn = func(snapshotID, sourceVolumeID string) ([]storageapitypes.SnapshotObject, *storageapitypes.Status, error) {
		if snapshotID != "snap-002" {
			t.Fatalf("showSnapshotsFn got snapshotID=%q, want snap-002", snapshotID)
		}
		return []storageapitypes.SnapshotObject{
			{ObjectName: "snapshot", Name: "snap-002", MasterVolumeName: "vol-a"},
		}, nil, nil
	}

	resp, err := controller.ListSnapshots(nil, &csi.ListSnapshotsRequest{SnapshotId: "backend-b|snap-002"})
	if err != nil {
		t.Fatalf("ListSnapshots returned error: %v", err)
	}

	wantCredentials := map[string]string{
		"apiAddress": "https://array-b",
		"username":   "user-b",
		"password":   "pass-b",
	}
	if len(configuredCredentials) != 1 || !reflect.DeepEqual(configuredCredentials[0], wantCredentials) {
		t.Fatalf("configured credentials = %v, want %v", configuredCredentials, wantCredentials)
	}
	if got := resp.GetEntries()[0].GetSnapshot().GetSnapshotId(); got != "backend-b|snap-002" {
		t.Fatalf("snapshot ID = %q, want backend-b|snap-002", got)
	}
}

func TestCreateSnapshotFailsWithoutBackendIDInMultiBackendMode(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
			"backend-b": {APIAddress: "https://array-b", Username: "user-b", Password: "pass-b"},
		},
		configureClientFn: func(credentials map[string]string) error { return nil },
	}
	controller.createSnapshotFn = func(sourceVolumeID, snapshotName string) (*storageapitypes.Status, error) {
		t.Fatal("createSnapshotFn should not be called without backendID")
		return nil, nil
	}

	_, err := controller.CreateSnapshot(nil, &csi.CreateSnapshotRequest{
		Name:           "snapshot-test",
		SourceVolumeId: "vol-a",
		Parameters:     map[string]string{},
	})
	if err == nil {
		t.Fatal("expected CreateSnapshot error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("grpc code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestListSnapshotsRejectsMalformedSnapshotID(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
		},
		configureClientFn: func(credentials map[string]string) error { return nil },
		showSnapshotsFn:   func(snapshotID, sourceVolumeID string) ([]storageapitypes.SnapshotObject, *storageapitypes.Status, error) { return nil, nil, nil },
	}

	_, err := controller.ListSnapshots(nil, &csi.ListSnapshotsRequest{SnapshotId: "backend-a|snap|bad"})
	if err == nil {
		t.Fatal("expected malformed snapshot ID error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("grpc code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestListSnapshotsWithoutSnapshotIDIsUnimplementedForMultiBackend(t *testing.T) {
	controller := &Controller{
		backendConfigs: map[string]BackendConfig{
			"backend-a": {APIAddress: "https://array-a", Username: "user-a", Password: "pass-a"},
			"backend-b": {APIAddress: "https://array-b", Username: "user-b", Password: "pass-b"},
		},
		configureClientFn: func(credentials map[string]string) error { return nil },
		showSnapshotsFn:   func(snapshotID, sourceVolumeID string) ([]storageapitypes.SnapshotObject, *storageapitypes.Status, error) { return nil, nil, nil },
	}

	_, err := controller.ListSnapshots(nil, &csi.ListSnapshotsRequest{})
	if err == nil {
		t.Fatal("expected ListSnapshots error")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("grpc code = %v, want %v", status.Code(err), codes.Unimplemented)
	}
}

func TestNewSnapshotFromResponseEmbedsBackendID(t *testing.T) {
	snapshot, err := newSnapshotFromResponse(&storageapitypes.SnapshotObject{
		ObjectName:       "snapshot",
		Name:             "snap-003",
		MasterVolumeName: "vol-a",
	}, "backend-a")
	if err != nil {
		t.Fatalf("newSnapshotFromResponse returned error: %v", err)
	}
	if snapshot.GetSnapshotId() != "backend-a|snap-003" {
		t.Fatalf("snapshot ID = %q, want backend-a|snap-003", snapshot.GetSnapshotId())
	}
}
