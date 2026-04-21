package storage

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"testing"
)

func TestResizeCommandForFs(t *testing.T) {
	tests := []struct {
		name      string
		fsType    string
		device    string
		volume    string
		command   string
		args      []string
		expectErr bool
	}{
		{
			name:    "xfs uses xfs_growfs against mount path",
			fsType:  "xfs",
			device:  "/dev/dm-0",
			volume:  "/var/lib/kubelet/pods/test/mount",
			command: "xfs_growfs",
			args:    []string{"/var/lib/kubelet/pods/test/mount"},
		},
		{
			name:    "ext4 uses resize2fs against device",
			fsType:  "ext4",
			device:  "/dev/dm-1",
			volume:  "/var/lib/kubelet/pods/test/mount",
			command: "resize2fs",
			args:    []string{"/dev/dm-1"},
		},
		{
			name:      "unsupported filesystem returns error",
			fsType:    "btrfs",
			device:    "/dev/dm-2",
			volume:    "/var/lib/kubelet/pods/test/mount",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, args, err := resizeCommandForFs(tt.fsType, tt.device, tt.volume)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error for fsType %q", tt.fsType)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if command != tt.command {
				t.Fatalf("command = %q, want %q", command, tt.command)
			}
			if !reflect.DeepEqual(args, tt.args) {
				t.Fatalf("args = %v, want %v", args, tt.args)
			}
		})
	}
}

func TestResizeFilesystemUsesDetectedFsType(t *testing.T) {
	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		cmd := exec.Command(os.Args[0], "-test.run=TestResizeFilesystemHelperProcess", "--", name)
		cmd.Args = append(cmd.Args, args...)
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		return cmd
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		cmd := exec.Command(os.Args[0], "-test.run=TestResizeFilesystemHelperProcess", "--", name)
		cmd.Args = append(cmd.Args, args...)
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		return cmd
	}

	if err := ResizeFilesystem("/dev/dm-3", "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/test/globalmount"); err != nil {
		t.Fatalf("ResizeFilesystem returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-3"},
		{"xfs_growfs", "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/test/globalmount"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestResizeFilesystemHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	args := os.Args
	commandIndex := 0
	for i, arg := range args {
		if arg == "--" {
			commandIndex = i + 1
			break
		}
	}
	if commandIndex == 0 || commandIndex >= len(args) {
		t.Fatalf("missing helper command args: %v", args)
	}

	switch args[commandIndex] {
	case "blkid":
		os.Stdout.WriteString("TYPE=xfs\n")
	case "xfs_growfs":
		os.Stdout.WriteString("resized\n")
	default:
		t.Fatalf("unexpected helper command: %v", args[commandIndex:])
	}
	os.Exit(0)
}
