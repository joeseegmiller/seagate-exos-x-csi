package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
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
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" && os.Getenv("GO_WANT_STORAGE_HELPER") != "1" {
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

	if os.Getenv("GO_WANT_STORAGE_HELPER") == "1" {
		runStorageHelperProcess(t, args[commandIndex:])
		return
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

func TestMountFilesystemInitializesBlankXFSVolume(t *testing.T) {
	t.Helper()

	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 2, "")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch name {
		case "mkfs.xfs", "xfs_repair", "mount":
			return helperCommand(t, 0, "ok")
		case "findmnt":
			return helperCommand(t, 1, "")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	req := &csi.NodePublishVolumeRequest{
		TargetPath: t.TempDir() + "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
		},
	}

	if err := MountFilesystem(req, "/dev/dm-0"); err != nil {
		t.Fatalf("MountFilesystem returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-0"},
		{"mkfs.xfs", "/dev/dm-0"},
		{"xfs_repair", "-n", "/dev/dm-0"},
		{"findmnt", "--output", "TARGET", "--noheadings", "/dev/dm-0"},
		{"mount", "-t", "xfs", "/dev/dm-0", req.GetTargetPath()},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestMountFilesystemInitializesBlankExt4Volume(t *testing.T) {
	t.Helper()

	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 2, "")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch name {
		case "mkfs.ext4", "e2fsck", "mount":
			return helperCommand(t, 0, "ok")
		case "findmnt":
			return helperCommand(t, 1, "")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	req := &csi.NodePublishVolumeRequest{
		TargetPath: t.TempDir() + "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
	}

	if err := MountFilesystem(req, "/dev/dm-1"); err != nil {
		t.Fatalf("MountFilesystem returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-1"},
		{"mkfs.ext4", "/dev/dm-1"},
		{"e2fsck", "-n", "/dev/dm-1"},
		{"findmnt", "--output", "TARGET", "--noheadings", "/dev/dm-1"},
		{"mount", "-t", "ext4", "/dev/dm-1", req.GetTargetPath()},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestMountFilesystemRetriesXFSWithNouuidOnDuplicateUUID(t *testing.T) {
	t.Helper()

	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=xfs\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case name == "xfs_repair":
			return helperCommand(t, 0, "checked")
		case name == "findmnt":
			return helperCommand(t, 1, "")
		case reflect.DeepEqual(command, []string{"mount", "-t", "xfs", "/dev/dm-2", "/target"}):
			return helperCommand(t, 1, "XFS (dm-2): Filesystem has duplicate UUID 12345678-1234-1234-1234-123456789abc - can't mount\n")
		case reflect.DeepEqual(command, []string{"mount", "-t", "xfs", "-o", "nouuid", "/dev/dm-2", "/target"}):
			return helperCommand(t, 0, "mounted with nouuid")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	req := &csi.NodePublishVolumeRequest{
		TargetPath: "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
		},
	}

	if err := MountFilesystem(req, "/dev/dm-2"); err != nil {
		t.Fatalf("MountFilesystem returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-2"},
		{"xfs_repair", "-n", "/dev/dm-2"},
		{"xfs_repair", "-n", "/dev/dm-2"},
		{"findmnt", "--output", "TARGET", "--noheadings", "/dev/dm-2"},
		{"mount", "-t", "xfs", "/dev/dm-2", "/target"},
		{"mount", "-t", "xfs", "-o", "nouuid", "/dev/dm-2", "/target"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestMountFilesystemDoesNotRetryXFSForNonDuplicateUUIDFailure(t *testing.T) {
	t.Helper()

	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=xfs\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case name == "xfs_repair":
			return helperCommand(t, 0, "checked")
		case name == "findmnt":
			return helperCommand(t, 1, "")
		case reflect.DeepEqual(command, []string{"mount", "-t", "xfs", "/dev/dm-4", "/target"}):
			return helperCommand(t, 1, "wrong fs type, bad option, bad superblock on /dev/dm-4")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	req := &csi.NodePublishVolumeRequest{
		TargetPath: "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
		},
	}

	if err := MountFilesystem(req, "/dev/dm-4"); err == nil {
		t.Fatal("expected xfs mount failure")
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-4"},
		{"xfs_repair", "-n", "/dev/dm-4"},
		{"xfs_repair", "-n", "/dev/dm-4"},
		{"findmnt", "--output", "TARGET", "--noheadings", "/dev/dm-4"},
		{"mount", "-t", "xfs", "/dev/dm-4", "/target"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestMountFilesystemDoesNotRetryExt4MountFailures(t *testing.T) {
	t.Helper()

	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=ext4\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case name == "e2fsck":
			return helperCommand(t, 0, "checked")
		case name == "findmnt":
			return helperCommand(t, 1, "")
		case reflect.DeepEqual(command, []string{"mount", "-t", "ext4", "/dev/dm-3", "/target"}):
			return helperCommand(t, 1, "wrong fs type, bad option, bad superblock on /dev/dm-3")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	req := &csi.NodePublishVolumeRequest{
		TargetPath: "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
	}

	if err := MountFilesystem(req, "/dev/dm-3"); err == nil {
		t.Fatal("expected ext4 mount failure")
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-3"},
		{"e2fsck", "-n", "/dev/dm-3"},
		{"e2fsck", "-n", "/dev/dm-3"},
		{"findmnt", "--output", "TARGET", "--noheadings", "/dev/dm-3"},
		{"mount", "-t", "ext4", "/dev/dm-3", "/target"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestEnsureFsTypeRecreatesInvalidDetectedXFS(t *testing.T) {
	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=xfs\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case reflect.DeepEqual(command, []string{"xfs_repair", "-n", "/dev/dm-5"}):
			return helperCommand(t, 1, "bad primary superblock")
		case reflect.DeepEqual(command, []string{"mkfs.xfs", "/dev/dm-5"}):
			return helperCommand(t, 0, "formatted")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	if err := EnsureFsType("xfs", "/dev/dm-5"); err != nil {
		t.Fatalf("EnsureFsType returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-5"},
		{"xfs_repair", "-n", "/dev/dm-5"},
		{"mkfs.xfs", "/dev/dm-5"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestEnsureFsTypeKeepsValidDetectedXFS(t *testing.T) {
	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=xfs\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case reflect.DeepEqual(command, []string{"xfs_repair", "-n", "/dev/dm-6"}):
			return helperCommand(t, 0, "ok")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	if err := EnsureFsType("xfs", "/dev/dm-6"); err != nil {
		t.Fatalf("EnsureFsType returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-6"},
		{"xfs_repair", "-n", "/dev/dm-6"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestEnsureFsTypeKeepsValidDetectedExt4(t *testing.T) {
	originalExecCommand := execCommand
	originalExecCommandContext := execCommandContext
	t.Cleanup(func() {
		execCommand = originalExecCommand
		execCommandContext = originalExecCommandContext
	})

	var commands [][]string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, append([]string{name}, args...))
		return helperCommand(t, 0, "TYPE=ext4\n")
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		command := append([]string{name}, args...)
		commands = append(commands, command)
		switch {
		case reflect.DeepEqual(command, []string{"e2fsck", "-n", "/dev/dm-7"}):
			return helperCommand(t, 0, "ok")
		default:
			t.Fatalf("unexpected command: %v", command)
			return nil
		}
	}

	if err := EnsureFsType("ext4", "/dev/dm-7"); err != nil {
		t.Fatalf("EnsureFsType returned error: %v", err)
	}

	want := [][]string{
		{"blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/dm-7"},
		{"e2fsck", "-n", "/dev/dm-7"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
}

func TestIsXFSNouuidMountError(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   bool
	}{
		{
			name:   "exact duplicate uuid mount failure",
			output: "XFS (dm-2): Filesystem has duplicate UUID 12345678-1234-1234-1234-123456789abc - can't mount\n",
			want:   true,
		},
		{
			name:   "duplicate uuid without cant mount",
			output: "XFS (dm-2): Filesystem has duplicate UUID 12345678-1234-1234-1234-123456789abc\n",
			want:   false,
		},
		{
			name:   "generic wrong fs type",
			output: "wrong fs type, bad option, bad superblock on /dev/dm-1",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isXFSNouuidMountError([]byte(tt.output)); got != tt.want {
				t.Fatalf("isXFSNouuidMountError(%q) = %v, want %v", tt.output, got, tt.want)
			}
		})
	}
}

func helperCommand(t *testing.T, exitCode int, stdout string) *exec.Cmd {
	t.Helper()

	return exec.Command(os.Args[0], "-test.run=TestStorageCommandHelper", "--", fmt.Sprintf("%d", exitCode), stdout)
}

func TestStorageCommandHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" || os.Getenv("GO_WANT_STORAGE_HELPER") == "1" {
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
	if commandIndex == 0 || len(args) < commandIndex+2 {
		return
	}

	if _, err := io.WriteString(os.Stdout, args[commandIndex+1]); err != nil {
		t.Fatalf("failed to write helper stdout: %v", err)
	}
	switch args[commandIndex] {
	case "0":
		os.Exit(0)
	case "1":
		os.Exit(1)
	case "2":
		os.Exit(2)
	default:
		t.Fatalf("unexpected helper exit code: %s", args[commandIndex])
	}
}
