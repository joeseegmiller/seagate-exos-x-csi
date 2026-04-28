package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Seagate/seagate-exos-x-csi/pkg/common"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const snapshotIDDelimiter = "|"

type BackendConfig struct {
	APIAddress  string `json:"apiAddress"`
	APIAddressB string `json:"apiAddressB,omitempty"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

func (config BackendConfig) credentials() map[string]string {
	credentials := map[string]string{
		common.APIAddressConfigKey: config.APIAddress,
		common.UsernameSecretKey:   config.Username,
		common.PasswordSecretKey:   config.Password,
	}
	if config.APIAddressB != "" {
		credentials[common.APIAddressBConfigKey] = config.APIAddressB
	}
	return credentials
}

func loadBackendConfigsFromFile(path string) (map[string]BackendConfig, error) {
	if path == "" {
		return map[string]BackendConfig{}, nil
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read backend config file: %w", err)
	}

	configs := map[string]BackendConfig{}
	if err := json.Unmarshal(payload, &configs); err != nil {
		return nil, fmt.Errorf("failed to parse backend config file: %w", err)
	}

	for backendID, config := range configs {
		if !isValidBackendID(backendID) {
			return nil, fmt.Errorf("invalid backend ID %q in backend config", backendID)
		}
		if config.APIAddress == "" || config.Username == "" || config.Password == "" {
			return nil, fmt.Errorf("backend %q is missing required controller credentials", backendID)
		}
	}

	return configs, nil
}

func formatSnapshotID(backendID, snapshotID string) string {
	if backendID == "" {
		return snapshotID
	}
	return backendID + snapshotIDDelimiter + snapshotID
}

func parseSnapshotID(id string) (backendID, snapshotID string, err error) {
	if id == "" {
		return "", "", fmt.Errorf("snapshot ID is required")
	}

	if strings.Count(id, snapshotIDDelimiter) != 1 {
		return "", "", fmt.Errorf("malformed snapshot ID %q", id)
	}

	delimiterIndex := strings.Index(id, snapshotIDDelimiter)
	backendID = id[:delimiterIndex]
	snapshotID = id[delimiterIndex+1:]
	if backendID == "" || snapshotID == "" {
		return "", "", fmt.Errorf("malformed snapshot ID %q", id)
	}

	if !isValidBackendID(backendID) {
		return "", "", fmt.Errorf("invalid backend ID %q in snapshot ID", backendID)
	}

	return backendID, snapshotID, nil
}

func isValidBackendID(id string) bool {
	if id == "" {
		return false
	}
	for _, ch := range id {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			continue
		}
		switch ch {
		case '-', '_', '.':
			continue
		default:
			return false
		}
	}
	return true
}

func (controller *Controller) backendConfigByID(backendID string) (BackendConfig, error) {
	if !isValidBackendID(backendID) {
		return BackendConfig{}, fmt.Errorf("invalid backend ID %q", backendID)
	}

	if len(controller.backendConfigs) == 0 {
		return BackendConfig{}, fmt.Errorf("controller backend credentials are required; ensure CONTROLLER_BACKEND_CONFIG_FILE is set")
	}

	config, ok := controller.backendConfigs[backendID]
	if !ok {
		return BackendConfig{}, fmt.Errorf("backend credentials not configured for backend %q", backendID)
	}

	return config, nil
}

func (controller *Controller) sortedBackendIDs() []string {
	backendIDs := make([]string, 0, len(controller.backendConfigs))
	for backendID := range controller.backendConfigs {
		backendIDs = append(backendIDs, backendID)
	}
	sort.Strings(backendIDs)
	return backendIDs
}

func (controller *Controller) resolveCreateSnapshotBackendID(parameters map[string]string) (string, error) {
	backendID := ""
	if parameters != nil {
		backendID = parameters[common.BackendIDConfigKey]
	}

	if backendID == "" {
		if len(controller.backendConfigs) == 1 {
			return controller.sortedBackendIDs()[0], nil
		}
		return "", fmt.Errorf("%s parameter is required when multiple backends are configured", common.BackendIDConfigKey)
	}

	if !isValidBackendID(backendID) {
		return "", fmt.Errorf("invalid backend ID %q", backendID)
	}

	if _, err := controller.backendConfigByID(backendID); err != nil {
		return "", err
	}

	return backendID, nil
}

func (controller *Controller) resolveCredentials(req *csi.CreateSnapshotRequest) (map[string]string, error) {
	if len(controller.backendConfigs) == 0 {
		return nil, fmt.Errorf("controller backend credentials are required; ensure CONTROLLER_BACKEND_CONFIG_FILE is set")
	}

	backendID, err := controller.resolveCreateSnapshotBackendID(req.GetParameters())
	if err != nil {
		return nil, err
	}
	config, err := controller.backendConfigByID(backendID)
	if err != nil {
		return nil, err
	}
	return config.credentials(), nil
}

func (controller *Controller) resolveCredentialsForSnapshotID(snapshotID string) (backendID, backendSnapshotID string, credentials map[string]string, err error) {
	backendID, backendSnapshotID, err = parseSnapshotID(snapshotID)
	if err != nil {
		return "", "", nil, status.Error(codes.InvalidArgument, err.Error())
	}

	config, err := controller.backendConfigByID(backendID)
	if err != nil {
		return "", "", nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return backendID, backendSnapshotID, config.credentials(), nil
}

func (controller *Controller) configureBackendByID(backendID string) error {
	config, err := controller.backendConfigByID(backendID)
	if err != nil {
		return err
	}
	return controller.configureClientFn(config.credentials())
}
