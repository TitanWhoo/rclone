package open115

import (
	"github.com/rclone/rclone/fstest/fstests"
	"testing"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "open115:",
		NilObject:  (*Object)(nil),
	})
}
