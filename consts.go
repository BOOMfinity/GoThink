package GoThink

import "github.com/hashicorp/go-version"

const (
	Version = "1.0.5"
)

var (
	Supported, _ = version.NewConstraint("=1.0.4,=1.0.5")
)
