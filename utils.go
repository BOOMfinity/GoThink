package GoThink

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

type DatabaseFlags struct {
	Host         string `long:"host" description:"RethinkDB address" default:"localhost"`
	Password     string `long:"pass" description:"RethinkDB admin user password"`
	PasswordFile string `long:"pass-file" description:"Path to the file with password"`
	Port         uint   `long:"port" description:"RethinkDB client driver port" default:"28015"`
}

type ExportFlags struct {
	Path string `long:"export" description:"What will be exported. Use database.table syntax."`
	File string `long:"file" description:"Backup filename or path" default:"backup.tar.gz"`
}

type ImportFlags struct {
	Import string `long:"import" description:"Use database.table syntax"`
	File   string `long:"file" description:"Backup filename or path" default:"backup.tar.gz"`
}
