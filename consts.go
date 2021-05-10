package GoThink

import "github.com/hashicorp/go-version"

const (
	Version = "0.0.1"
)

var (
	Supported, _ = version.NewConstraint("<=0.0.1")
)
