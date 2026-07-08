package activities

import (
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/veelabs/dev-environments/provisioner/internal/config"
)

// TestStructRegistration registers the Activities struct exactly as
// cmd/worker/main.go does. RegisterActivity(struct) treats every exported
// method as an activity and panics on invalid signatures — a class of failure
// that only appears at worker startup unless covered here.
func TestStructRegistration(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("registering Activities struct panicked: %v", r)
		}
	}()
	env.RegisterActivity(New(config.Config{BaseDomain: "example.test"}, nil, nil))
}
