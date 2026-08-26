package version

import (
	"os"
	"strings"
)

// Version is loaded from the repository VERSION file at runtime. The Docker image
// copies that same file into /usr/local/share/dsh-manager/VERSION.
var Version = load()

func load() string {
	paths := []string{
		os.Getenv("DSH_MANAGER_VERSION_FILE"),
		"/usr/local/share/dsh-manager/VERSION",
		"./VERSION",
	}
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			return strings.TrimSpace(string(data))
		}
	}
	return "dev"
}
