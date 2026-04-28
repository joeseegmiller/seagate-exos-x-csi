package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	storageapitypes "github.com/Seagate/seagate-exos-x-api-go/v2/pkg/common"
	"github.com/Seagate/seagate-exos-x-csi/pkg/common"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// CreateSnapshot creates a snapshot of the given volume
func (controller *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	parameters := req.GetParameters()
	backendID, err := controller.resolveCreateSnapshotBackendID(parameters)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	config, err := controller.backendConfigByID(backendID)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	credentials := config.credentials()
	snapshotName, err := common.TranslateName(req.GetName(), parameters[common.VolumePrefixKey])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "translate snapshot name contains invalid characters")
	}

	if common.ValidateName(snapshotName) == false {
		return nil, status.Error(codes.InvalidArgument, "snapshot name contains invalid characters")
	}

	sourceVolumeId, err := common.VolumeIdGetName(req.GetSourceVolumeId())
	if sourceVolumeId == "" || err != nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot SourceVolumeId is not valid")
	}

	if err := controller.configureClientFn(credentials); err != nil {
		return nil, err
	}

	err = controller.createSnapshotFn(sourceVolumeId, snapshotName)
	if err != nil {
		return nil, err
	}

	// The expectation is that show snapshots will return a single array item for the snapshot created
	snapshots, err := controller.showSnapshotsFn(snapshotName, "")
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

	backendSnapshotID, err := controller.prepareDeleteSnapshotClient(req)
	if err != nil {
		return nil, err
	}

	err = controller.deleteSnapshotFn(backendSnapshotID)
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

	snapshots, err := controller.listSnapshotEntries(req, sourceVolumeId)
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

func (controller *Controller) prepareDeleteSnapshotClient(req *csi.DeleteSnapshotRequest) (string, error) {
	_, backendSnapshotID, err := parseSnapshotID(req.GetSnapshotId())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}

	_, _, credentials, err := controller.resolveCredentialsForSnapshotID(req.GetSnapshotId())
	if err != nil {
		return "", err
	}
	if err := controller.configureClientFn(credentials); err != nil {
		return "", err
	}
	return backendSnapshotID, nil
}

func (controller *Controller) listSnapshotEntries(req *csi.ListSnapshotsRequest, sourceVolumeId string) ([]*csi.ListSnapshotsResponse_Entry, error) {
	if req.GetSnapshotId() != "" {
		backendID, backendSnapshotID, credentials, err := controller.resolveCredentialsForSnapshotID(req.GetSnapshotId())
		if err != nil {
			return nil, err
		}
		return controller.listSnapshotEntriesForBackend(backendID, backendSnapshotID, sourceVolumeId, credentials)
	}

	if len(controller.backendConfigs) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "controller backend credentials are required; ensure CONTROLLER_BACKEND_CONFIG_FILE is set")
	}

	if len(controller.backendConfigs) > 1 {
		return nil, status.Error(codes.Unimplemented, "ListSnapshots without SnapshotId is not supported with multiple backends")
	}

	backendID := controller.sortedBackendIDs()[0]
	return controller.listSnapshotEntriesForBackend(backendID, "", sourceVolumeId, controller.backendConfigs[backendID].credentials())
}

func (controller *Controller) listSnapshotEntriesForBackend(backendID, snapshotID, sourceVolumeId string, credentials map[string]string) ([]*csi.ListSnapshotsResponse_Entry, error) {
	if err := controller.configureClientFn(credentials); err != nil {
		return nil, err
	}

	response, err := controller.showSnapshotsFn(snapshotID, sourceVolumeId)
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
