package _import

import (
	"archive/tar"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BOOMfinity-Developers/GoThink"
	"github.com/BOOMfinity-Developers/GoThink/pkg"
	"github.com/hashicorp/go-version"
	"github.com/klauspost/compress/gzip"
	"github.com/urfave/cli/v2"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func RunFromCLI(ctx *cli.Context) error {
	return Run(ctx.Context.Value("database").(*rethinkdb.Session), ctx.String("file"), ctx.String("import-path"))
}

func Run(DB *rethinkdb.Session, filePath, importPath string) error {
	var (
		workers   = newWorkerPool()
		dst, _    = ioutil.TempDir(os.TempDir(), "gothink.import.*")
		start     = time.Now()
		data      = ParseImportPath(importPath)
		importP   string
		databases []string
	)
	workers.Spawn(0)
	if data.Database != "" {
		importP = filepath.Join(data.Database, data.Table)
	}
	if err := UnzipFile(filePath, dst, importP); err != nil {
		return err
	}
	rethinkdb.DBList().ReadAll(&databases, DB)
	dbs, _ := os.ReadDir(dst)
	for _, db := range dbs {
		im := NewDatabaseImport(db.Name(), DB, workers, databases)
		im.SetDestination(dst)
		if err := im.Run(); err != nil {
			return err
		}
	}

	println()
	log.Printf("Imported in %v", time.Now().Sub(start).String())
	println()

	os.RemoveAll(dst)
	return nil
}

func UnzipFile(filePath string, dst string, importPath string) error {
	found := false
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder, _ := gzip.NewReader(file)
	reader := tar.NewReader(decoder)
	var ver *version.Version
	for {
		header, err := reader.Next()

		if err == io.EOF {
			break
		}

		switch {
		case err != nil:
			panic(err)
		case header == nil:
			continue
		}
		if header.Name == ".version" {
			data, _ := io.ReadAll(reader)
			ver, _ = version.NewVersion(string(data))
			if !GoThink.Supported.Check(ver) {
				log.Fatalf("This version of GoThink (%v) doesn't support backups from GoThink v%v. To continue, please download the older CLI version that supports this backup version.", GoThink.Version, ver.String())
			}
			continue
		}
		if !strings.HasPrefix(header.Name, importPath) {
			continue
		}
		found = true
		target := filepath.Join(dst, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					panic(err)
				}
			}
		case tar.TypeReg:
			f, err := os.Create(target)
			if err != nil {
				panic(err)
			}
			if _, err := io.Copy(f, reader); err != nil {
				panic(err)
			}
			f.Close()
		}
	}
	if !found {
		return errors.New("Database or table not found. Check that the -i flag is set correctly.")
	}
	return nil
}

func ParseImportPath(path string) (res pkg.ToExport) {
	if path == "" {
		log.Println("An import path not specified. Importing all data.")
		res.All = true
		return
	}
	str := strings.Split(path, ".")
	if len(str) == 2 {
		res.Table = str[1]
	}
	res.Database = str[0]
	return
}
