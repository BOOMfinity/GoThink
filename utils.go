package rethinkgo_backups

import (
	"fmt"
)

type PowerfulStringSlice []string

func (s PowerfulStringSlice) Filter(cb func(a string) bool) (r PowerfulStringSlice) {
	for _, x := range s {
		if cb(x) {
			r = append(r, x)
		}
	}
	return
}

func ReadableByteCount(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
