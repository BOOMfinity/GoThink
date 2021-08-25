package GoThink

import "github.com/hashicorp/go-version"

const (
	Version = "1.0.3"
)

var (
	Supported, _ = version.NewConstraint(">=1.0.2,<=1.0.3")
)
