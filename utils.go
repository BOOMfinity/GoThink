package rethinkgo_backups

type PowerfulStringSlice []string

func (s PowerfulStringSlice) Filter(cb func(a string) bool) (r PowerfulStringSlice) {
	for _, x := range s {
		if cb(x) {
			r = append(r, x)
		}
	}
	return
}
