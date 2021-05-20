package GoThink

import "github.com/hashicorp/go-version"

const (
	Version = "1.0.0"
)

var (
	Supported, _ = version.NewConstraint("<=1.0.0")
)
