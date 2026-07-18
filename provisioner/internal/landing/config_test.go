package landing

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHermesGitAllowedHostsConfig(t *testing.T) {
	t.Setenv("LANDING_KIND", "hermes")
	t.Setenv("CLAIM_TTL", "1h")
	t.Setenv("MAX_CONCURRENT", "2")

	t.Run("defaults to GitHub", func(t *testing.T) {
		old, set := os.LookupEnv("HERMES_GIT_ALLOWED_HOSTS")
		require.NoError(t, os.Unsetenv("HERMES_GIT_ALLOWED_HOSTS"))
		t.Cleanup(func() {
			if set {
				_ = os.Setenv("HERMES_GIT_ALLOWED_HOSTS", old)
			} else {
				_ = os.Unsetenv("HERMES_GIT_ALLOWED_HOSTS")
			}
		})

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, []string{"github.com"}, cfg.HermesGitAllowedHosts)
	})

	t.Run("canonicalizes configured hosts", func(t *testing.T) {
		t.Setenv("HERMES_GIT_ALLOWED_HOSTS", " GitHub.COM, profiles.Example.com ")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, []string{"github.com", "profiles.example.com"}, cfg.HermesGitAllowedHosts)
	})

	for _, value := range []string{"", "github.com,", "-invalid.example", "127.0.0.1", "github.com:443"} {
		t.Run("rejects "+value, func(t *testing.T) {
			t.Setenv("HERMES_GIT_ALLOWED_HOSTS", value)
			_, err := LoadConfig()
			require.ErrorContains(t, err, "HERMES_GIT_ALLOWED_HOSTS")
		})
	}
}
