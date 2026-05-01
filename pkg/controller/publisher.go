package controller

import (
	"context"
	"fmt"
	"sort"
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

// ControllerPublishVolume attaches the given volume to the node
func (driver *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot publish volume with empty ID")
	}
	if len(req.GetNodeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot publish volume to a node with empty ID")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "cannot publish volume without capabilities")
	}

	nodeIP := req.GetNodeId()
	parameters := req.GetVolumeContext()

	initiators, err := driver.GetNodeInitiators(ctx, nodeIP, parameters[common.StorageProtocolKey])
	if err != nil {
		klog.ErrorS(err, "error getting node initiators", "node-ip", nodeIP, "storage-protocol", parameters[common.StorageProtocolKey])
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Could not retrieve initiators for scheduled node(%s)", nodeIP))
	}

	volumeName, _ := common.VolumeIdGetName(req.GetVolumeId())
	inFlightKey := volumeName + "|" + nodeIP
	driver.inFlightMu.Lock()
	driver.inFlightPublishes[inFlightKey]++
	driver.inFlightMu.Unlock()
	defer func() {
		driver.inFlightMu.Lock()
		defer driver.inFlightMu.Unlock()

		if count, ok := driver.inFlightPublishes[inFlightKey]; ok {
			count--
			if count <= 0 {
				delete(driver.inFlightPublishes, inFlightKey)
			} else {
				driver.inFlightPublishes[inFlightKey] = count
			}
		}
	}()

	klog.InfoS("attach request", "initiator(s)", initiators, "volume", volumeName)
	driver.recordKnownInitiators(initiators)
	backendID, err := driver.resolveBackendIDForVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	apiClient, err := driver.getConfiguredClient(ctx, backendID)
	if err != nil {
		return nil, err
	}

	lun, err := driver.publishVolumeWithRetry(apiClient, volumeName, initiators)
	if err != nil {
		return nil, err
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{"lun": lun},
	}, err
}

// ControllerUnpublishVolume detaches the given volume from the node
func (driver *Controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot unpublish volume with empty ID")
	}

	volumeName, _ := common.VolumeIdGetName(req.GetVolumeId())
	nodeIP := req.GetNodeId()
	inFlightKey := volumeName + "|" + nodeIP
	driver.inFlightMu.Lock()
	inFlight := driver.inFlightPublishes[inFlightKey]
	driver.inFlightMu.Unlock()
	if inFlight > 0 {
		klog.V(1).InfoS("skipping unmap; publish still in progress",
			"volumeName", volumeName,
			"nodeID", nodeIP,
			"inFlightPublishes", inFlight,
		)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	volumeWWN, _ := common.VolumeIdGetWwn(req.GetVolumeId())
	storageProtocol, err := common.VolumeIdGetStorageProtocol(req.GetVolumeId())
	if err != nil {
		klog.ErrorS(err, "No storage protocol found in ControllerUnpublishVolume", "storage protocol", storageProtocol, "volume ID:", req.GetVolumeId())
		return nil, err
	}

	initiators, err := driver.GetNodeInitiators(ctx, nodeIP, storageProtocol)
	if err != nil {
		klog.ErrorS(err, "error getting initiators from the node", "nodeIP", nodeIP, "storageProtocol", storageProtocol)
	}
	driver.recordKnownInitiators(initiators)
	backendID, err := driver.resolveBackendIDForVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	apiClient, err := driver.getConfiguredClient(ctx, backendID)
	if err != nil {
		return nil, err
	}

	klog.InfoS("unmapping volume from initiator", "volumeName", volumeName, "initiators", initiators)
	for _, initiator := range initiators {
		status, err := apiClient.UnmapVolume(volumeName, initiator)
		if err != nil {
			if status != nil && status.ReturnCode == storageapitypes.UnmapFailedErrorCode {
				klog.Info("unmap failed, assuming volume is already unmapped")
			} else {
				klog.Errorf("unknown error while unmapping initiator %s: %v", initiator, err)
			}
		} else {
			driver.NotifyUnmap(ctx, nodeIP, volumeWWN)
		}
	}

	klog.Infof("successfully unmapped volume %s from all initiators", volumeName)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (driver *Controller) recordKnownInitiators(initiators []string) {
	driver.knownInitiatorsMu.Lock()
	defer driver.knownInitiatorsMu.Unlock()
	for _, initiator := range initiators {
		if initiator == "" {
			continue
		}
		driver.knownInitiators[initiator] = struct{}{}
	}
}

func (driver *Controller) getKnownInitiators(excluded map[string]struct{}) []string {
	driver.knownInitiatorsMu.RLock()
	defer driver.knownInitiatorsMu.RUnlock()

	initiators := make([]string, 0, len(driver.knownInitiators))
	for initiator := range driver.knownInitiators {
		if _, skip := excluded[initiator]; skip {
			continue
		}
		initiators = append(initiators, initiator)
	}
	sort.Strings(initiators)
	return initiators
}

func (driver *Controller) publishVolumeWithRetry(apiClient *storageapi.Client, volumeName string, initiators []string) (string, error) {
	candidateLUN, err := driver.choosePublishLUN(apiClient, initiators, volumeName)
	if err != nil {
		return "", err
	}
	klog.V(1).InfoS("selected candidate LUN", "volumeName", volumeName, "lun", candidateLUN)

	maxRetryLUN := candidateLUN + 9
	if maxRetryLUN >= storageapi.ApiMaximumLUN {
		maxRetryLUN = storageapi.ApiMaximumLUN - 1
	}

	for lun := candidateLUN; lun <= maxRetryLUN; lun++ {
		if lun != candidateLUN {
			klog.V(1).InfoS("retrying mapping with LUN", "volumeName", volumeName, "lun", lun)
		}
		klog.V(1).InfoS("attempting volume mapping", "volumeName", volumeName, "initiators", initiators, "lun", lun)
		actualLUN, err := driver.mapVolumeToInitiators(apiClient, volumeName, initiators, lun)
		if err != nil {
			if isLUNAllocationFailure(err) {
				klog.V(1).ErrorS("mapping failed due to LUN conflict, retrying", "volumeName", volumeName, "lun", lun, "err", err)
				continue
			}
			return "", err
		}
		return strconv.Itoa(actualLUN), nil
	}

	return "", status.Errorf(codes.ResourceExhausted, "failed to map volume %s after retrying LUNs %d-%d", volumeName, candidateLUN, maxRetryLUN)
}

func (driver *Controller) mapVolumeToInitiators(apiClient *storageapi.Client, volumeName string, initiators []string, lun int) (int, error) {
	newlyMappedInitiators := make([]string, 0, len(initiators))
	for _, initiator := range initiators {
		targetLUN := lun

		klog.V(1).InfoS("ensuring mapping for initiator", "volumeName", volumeName, "initiator", initiator, "lun", targetLUN)
		respStatus, err := apiClient.MapVolume(volumeName, initiator, "rw", targetLUN)
		driver.logHostMapsForInitiator(apiClient, volumeName, initiator)

		if respStatus != nil && respStatus.ReturnCode == storageapitypes.VolumeNotFoundErrorCode {
			driver.cleanupMappedInitiators(apiClient, volumeName, newlyMappedInitiators)
			return -1, status.Errorf(codes.NotFound, "mapping failed: returnCode=%d response=%s", respStatus.ReturnCode, respStatus.Response)
		}

		alreadyMapped := respStatus != nil &&
			respStatus.ReturnCode == storageapitypes.LUNOverlapErrorCode &&
			strings.Contains(strings.ToLower(respStatus.Response), "already mapped")
		if respStatus != nil && respStatus.ReturnCode == storageapitypes.LUNOverlapErrorCode && !alreadyMapped {
			driver.cleanupMappedInitiators(apiClient, volumeName, newlyMappedInitiators)
			return -1, status.Errorf(codes.AlreadyExists, "mapping failed: returnCode=%d response=%s", respStatus.ReturnCode, respStatus.Response)
		}
		if err != nil {
			driver.cleanupMappedInitiators(apiClient, volumeName, newlyMappedInitiators)
			return -1, preserveStatusOr(codes.Internal, err)
		}
		if respStatus != nil && respStatus.ReturnCode != 0 && !alreadyMapped {
			driver.cleanupMappedInitiators(apiClient, volumeName, newlyMappedInitiators)
			return -1, status.Errorf(codes.Internal, "mapping failed: returnCode=%d response=%s", respStatus.ReturnCode, respStatus.Response)
		}

		if !containsString(newlyMappedInitiators, initiator) {
			newlyMappedInitiators = append(newlyMappedInitiators, initiator)
		}
		responseText := ""
		if respStatus != nil {
			responseText = respStatus.Response
		}
		authoritativeLUN, hasAuthoritativeLUN := mappedLUNFromResponse(responseText)
		if alreadyMapped {
			if hasAuthoritativeLUN {
				klog.V(1).InfoS("using backend-confirmed LUN", "volumeName", volumeName, "initiator", initiator, "lun", authoritativeLUN)
			} else {
				klog.V(1).InfoS("using fallback LUN", "volumeName", volumeName, "initiator", initiator, "lun", targetLUN)
			}
			klog.V(1).InfoS("mapping treated as success", "volumeName", volumeName, "initiator", initiator, "lun", targetLUN, "returnCode", responseReturnCode(respStatus), "response", respStatus.Response)
			continue
		}

		if hasAuthoritativeLUN {
			klog.V(1).InfoS("using backend-confirmed LUN", "volumeName", volumeName, "initiator", initiator, "lun", authoritativeLUN)
		} else {
			klog.V(1).InfoS("using fallback LUN", "volumeName", volumeName, "initiator", initiator, "lun", targetLUN)
		}
		if respStatus != nil && respStatus.ReturnCode == 0 {
			klog.V(1).InfoS("mapping treated as success", "volumeName", volumeName, "initiator", initiator, "lun", targetLUN, "returnCode", responseReturnCode(respStatus), "response", respStatus.Response)
		}
	}

	if len(newlyMappedInitiators) == 0 {
		return -1, status.Error(codes.Internal, "no LUN assigned after mapping")
	}

	klog.InfoS("successfully mapped volume to all initiators", "volumeName", volumeName, "initiators", initiators, "lun", lun)
	return lun, nil
}

func (driver *Controller) logHostMapsForInitiator(apiClient *storageapi.Client, volumeName, initiator string) {
	volumes, _, err := apiClient.ShowHostMaps(initiator)
	if err != nil {
		klog.V(1).ErrorS(err, "ShowHostMaps informational logging failed", "volumeName", volumeName, "initiator", initiator)
		return
	}

	summaries := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		summaries = append(summaries, fmt.Sprintf("name=%q lun=%d", strings.TrimSpace(volume.Name), volume.LUN))
	}
	klog.V(2).InfoS("ShowHostMaps informational result only", "volumeName", volumeName, "initiator", initiator, "hostMaps", summaries)
}

func mappedLUNFromResponse(response string) (int, bool) {
    lower := strings.ToLower(response)
    idx := strings.Index(lower, "lun")
    if idx == -1 {
        return 0, false
    }

    // scan after "lun"
    for i := idx; i < len(response); i++ {
        if response[i] >= '0' && response[i] <= '9' {
            j := i
            for j < len(response) && response[j] >= '0' && response[j] <= '9' {
                j++
            }
            lun, err := strconv.Atoi(response[i:j])
            if err == nil {
                return lun, true
            }
            break
        }
    }

    return 0, false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (driver *Controller) cleanupMappedInitiators(apiClient *storageapi.Client, volumeName string, initiators []string) {
	for _, initiator := range initiators {
		respStatus, err := apiClient.UnmapVolume(volumeName, initiator)
		if respStatus != nil && respStatus.ReturnCode == storageapitypes.VolumeNotFoundErrorCode {
			continue
		}
		if err != nil {
			klog.V(1).ErrorS(err, "failed to cleanup partial mapping", "volumeName", volumeName, "initiator", initiator, "returnCode", responseReturnCode(respStatus))
		}
	}
}

func (driver *Controller) choosePublishLUN(apiClient *storageapi.Client, initiators []string, volumeName string) (int, error) {
	allVolumes := make([]storageapi.Volume, 0, 32)
	for _, initiator := range initiators {
		volumes, _, err := apiClient.ShowHostMaps(initiator)
		if err != nil {
			klog.ErrorS(err, "error looking for host maps", "initiator", initiator)
			continue
		}
		allVolumes = append(allVolumes, volumes...)
	}

	sort.Slice(allVolumes, func(i, j int) bool {
		return allVolumes[i].LUN < allVolumes[j].LUN
	})

	existingLUN := -1
	for _, volume := range allVolumes {
		if volume.Name != volumeName {
			continue
		}
		if existingLUN < 0 {
			existingLUN = volume.LUN
			continue
		}
		if existingLUN != volume.LUN {
			return -1, fmt.Errorf("found multiple LUNs (%d, %d) for volume %s", existingLUN, volume.LUN, volumeName)
		}
	}
	if existingLUN >= 0 {
		return existingLUN, nil
	}
	if len(allVolumes) == 0 {
		return 1, nil
	}
	if allVolumes[len(allVolumes)-1].LUN+1 < storageapi.ApiMaximumLUN {
		return allVolumes[len(allVolumes)-1].LUN + 1, nil
	}
	if allVolumes[0].LUN > 1 {
		return 1, nil
	}
	for index := 1; index < len(allVolumes); index++ {
		if allVolumes[index].LUN-allVolumes[index-1].LUN > 1 {
			return allVolumes[index-1].LUN + 1, nil
		}
	}
	return -1, status.Error(codes.ResourceExhausted, "no more available LUNs")
}

func isLUNAllocationFailure(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

func responseReturnCode(respStatus *storageapitypes.ResponseStatus) int {
	if respStatus == nil {
		return 0
	}
	return respStatus.ReturnCode
}
