package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	storageapi "github.com/Seagate/seagate-exos-x-api-go/v2/pkg/api"
	storageapitypes "github.com/Seagate/seagate-exos-x-api-go/v2/pkg/common"
	"github.com/Seagate/seagate-exos-x-csi/pkg/common"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

var invalidSnapshotNameChars = regexp.MustCompile(`[^a-z0-9-]`)

const maxSnapshotNameLength = 32

func sanitizeName(s string) string {
	s = strings.ToLower(s)
	return invalidSnapshotNameChars.ReplaceAllString(s, "-")
}

// CreateSnapshot creates a snapshot of the given volume
func (controller *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	parameters := req.GetParameters()
	backendID, err := controller.resolveCreateSnapshotBackendID(parameters)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	apiClient, err := controller.getConfiguredClient(ctx, backendID)
	if err != nil {
		return nil, err
	}

	sourceVolumeId, err := common.VolumeIdGetName(req.GetSourceVolumeId())
	if sourceVolumeId == "" || err != nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot SourceVolumeId is not valid")
	}

	namePart := sanitizeName(req.GetName())
	if namePart == "" {
		namePart = "snap"
	}
	if len(namePart) > 32 {
		namePart = namePart[:32]
	}

	// Note: StorageClass parameters (pool, storageProtocol, volPrefix) are not
	// available during CreateSnapshot. Snapshot creation operates on an existing
	// volume, so backend determines pool and protocol from the source volume.
	snapshotName := fmt.Sprintf("%s-%s", sourceVolumeId, namePart)

	if len(snapshotName) > maxSnapshotNameLength {
		suffix := "-" + namePart
		maxBaseLen := maxSnapshotNameLength - len(suffix)

		if maxBaseLen < 1 {
			// fallback: trim suffix instead
			maxSuffixLen := maxSnapshotNameLength - 2
			if maxSuffixLen < 1 {
				maxSuffixLen = 1
			}
			if len(namePart) > maxSuffixLen {
				namePart = namePart[:maxSuffixLen]
			}
			snapshotName = fmt.Sprintf("%s-%s", sourceVolumeId[:1], namePart)
		} else {
			base := sourceVolumeId
			if len(base) > maxBaseLen {
				base = base[:maxBaseLen]
			}
			snapshotName = base + suffix
		}
	}
	if !common.ValidateName(snapshotName) {
		return nil, status.Error(codes.InvalidArgument, "snapshot name contains invalid characters")
	}
	klog.Infof("creating snapshot %q for volume %q", snapshotName, sourceVolumeId)

	err = controller.createSnapshotFn(apiClient, sourceVolumeId, snapshotName)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return nil, err
		}
	}

	// The expectation is that show snapshots will return a single array item for the snapshot created
	snapshots, err := controller.showSnapshotsFn(apiClient, snapshotName, "")
	if err != nil {
		return nil, err
	}

	var snapshot *csi.Snapshot
	for _, ss := range snapshots {
		if ss.ObjectName != "snapshot" {
			continue
		}

		snapshot, err = newSnapshotFromResponse(&ss, backendID)
		if err != nil {
			return nil, err
		}
	}

	if snapshot == nil {
		return nil, errors.New("snapshot not found")
	}

	if snapshot.SourceVolumeId != sourceVolumeId {
		return nil, status.Error(codes.AlreadyExists, "cannot validate volume with empty ID")
	}

	return &csi.CreateSnapshotResponse{Snapshot: snapshot}, nil
}

// DeleteSnapshot deletes a snapshot of the given volume
func (controller *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {

	if req.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteSnapshot snapshot id is required")
	}

	backendID, backendSnapshotID, err := controller.prepareDeleteSnapshotClient(req)
	if err != nil {
		return nil, err
	}
	apiClient, err := controller.getConfiguredClient(ctx, backendID)
	if err != nil {
		return nil, err
	}

	err = controller.deleteSnapshotFn(apiClient, backendSnapshotID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			klog.Infof("snapshot %s does not exist, assuming it has already been deleted", req.SnapshotId)
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, err
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots: list existing snapshots up to MaxEntries
func (controller *Controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	sourceVolumeId, err := common.VolumeIdGetName(req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot SourceVolumeId is not valid")
	}

	// StartingToken is an index from 1 to maximum, "" returns 0
	startingToken, err := strconv.Atoi(req.StartingToken)
	klog.V(2).Infof("ListSnapshots: MaxEntries=%v, StartingToken=%q|%d", req.MaxEntries, req.StartingToken, startingToken)

	snapshots, err := controller.listSnapshotEntries(ctx, req, sourceVolumeId)
	if err != nil {
		return nil, err
	}

	window := []*csi.ListSnapshotsResponse_Entry{}
	var count, total, next int32 = 0, 0, math.MaxInt32

	for _, entry := range snapshots {
		snapshot := entry.Snapshot
		total++
		klog.V(2).Infof("snapshot[%d]: SnapshotId=%v, SourceVolumeId=%v", total, snapshot.SnapshotId, snapshot.SourceVolumeId)

		if (req.StartingToken == "") || (req.StartingToken != "" && total >= int32(startingToken)) {
			if (req.MaxEntries == 0) || (count < req.MaxEntries) {
				window = append(window, entry)
				count++
				klog.V(2).Infof("   added[%d]: SnapshotId=%v, SourceVolumeId=%v", count, snapshot.SnapshotId, snapshot.SourceVolumeId)
			}
			if (req.MaxEntries != 0) && (count == req.MaxEntries) && (next == math.MaxInt32) {
				next = total + 1
				klog.V(2).Infof("next=%v", next)
			}
		}
	}

	klog.V(2).Infof("ListSnapshots[%d]: %v", count, window)

	// Mark the next token if there are snapshot entries remaining
	nextToken := ""
	if (req.MaxEntries != 0) && (next <= total) {
		nextToken = strconv.FormatInt(int64(next), 10)
		klog.V(2).Infof("next=%v, nextToken=%q", next, nextToken)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   window,
		NextToken: nextToken,
	}, nil
}

func (controller *Controller) prepareDeleteSnapshotClient(req *csi.DeleteSnapshotRequest) (string, string, error) {
	backendID, backendSnapshotID, err := parseSnapshotID(req.GetSnapshotId())
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	return backendID, backendSnapshotID, nil
}

func (controller *Controller) listSnapshotEntries(ctx context.Context, req *csi.ListSnapshotsRequest, sourceVolumeId string) ([]*csi.ListSnapshotsResponse_Entry, error) {
	if req.GetSnapshotId() != "" {
		backendID, backendSnapshotID, err := parseSnapshotID(req.GetSnapshotId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		apiClient, err := controller.getConfiguredClient(ctx, backendID)
		if err != nil {
			return nil, err
		}
		return controller.listSnapshotEntriesForBackend(apiClient, backendID, backendSnapshotID, sourceVolumeId)
	}

	if len(controller.backendConfigs) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "controller backend credentials are required; ensure CONTROLLER_BACKEND_CONFIG_FILE is set")
	}

	if len(controller.backendConfigs) > 1 {
		return nil, status.Error(codes.Unimplemented, "ListSnapshots without SnapshotId is not supported with multiple backends")
	}

	backendID := controller.sortedBackendIDs()[0]
	apiClient, err := controller.getConfiguredClient(ctx, backendID)
	if err != nil {
		return nil, err
	}
	return controller.listSnapshotEntriesForBackend(apiClient, backendID, "", sourceVolumeId)
}

func (controller *Controller) listSnapshotEntriesForBackend(apiClient *storageapi.Client, backendID, snapshotID, sourceVolumeId string) ([]*csi.ListSnapshotsResponse_Entry, error) {
	response, err := controller.showSnapshotsFn(apiClient, snapshotID, sourceVolumeId)
	if err != nil {
		return nil, err
	}

	entries := []*csi.ListSnapshotsResponse_Entry{}
	for _, object := range response {
		snapshot, err := newSnapshotFromResponse(&object, backendID)
		if err == nil {
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: snapshot})
		}
	}

	return entries, nil
}

func newSnapshotFromResponse(snapshot *storageapitypes.SnapshotObject, backendID string) (*csi.Snapshot, error) {
	if snapshot.ObjectName != "snapshot" {
		return nil, fmt.Errorf("not a snapshot object, type is %v", snapshot.ObjectName)
	}

	klog.InfoS("csi snapshot info", "snapshot", snapshot.Name, "volume", snapshot.MasterVolumeName, "creationTime", snapshot.CreationTime)

	return &csi.Snapshot{
		SizeBytes:      snapshot.TotalSizeNumeric,
		SnapshotId:     formatSnapshotID(backendID, snapshot.Name),
		SourceVolumeId: snapshot.MasterVolumeName,
		CreationTime:   snapshot.CreationTime,
		ReadyToUse:     true,
	}, nil
}
