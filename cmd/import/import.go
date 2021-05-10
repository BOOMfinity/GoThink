package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"GoThink"
	"GoThink/database"
	"github.com/hashicorp/go-version"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

var (
	FilePath      = flag.String("file", "", "Path to the backup file")
	ImportPath    = flag.String("i", "", "Use database.table syntax")
	ImportAll     = false
	TableToImport = ""
	DBToImport    = ""
	dst           = ""

	databases []string
	workers   = newWorkerPool()
)

func main() {
	println()
	println("Welcome to RethinkGO-Backups CLI v" + GoThink.Version)
	println()
	flag.Parse()
	c, err := database.NewConnection()
	if err != nil {
		panic(err)
	}
	println()
	dst, err = ioutil.TempDir(os.TempDir(), "gothink.import.*")
	if err != nil {
		panic(err)
	}
	start := time.Now()
	file, err := os.Open(*FilePath)
	if err != nil {
		panic(err)
	}
	parseImportPath()
	decoder, _ := gzip.NewReader(file)
	reader := tar.NewReader(decoder)
	var ver *version.Version

	workers.Spawn(0)
	var (
		importP string
		found   = false
	)
	if DBToImport != "" {
		importP = filepath.Join(DBToImport, TableToImport)
	}

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
		if !strings.HasPrefix(header.Name, importP) {
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
		log.Fatalf("Database or table not found. Check that the -i flag is set correctly.")
	}
	r.DBList().ReadAll(&databases, c.DB)

	dbs, _ := os.ReadDir(dst)

	for _, db := range dbs {
		im := newDatabaseImport(db.Name(), c)
		im.Run()
	}

	println()
	log.Printf("Imported in %v", time.Now().Sub(start).String())
	println()

	os.RemoveAll(dst)
}

func parseImportPath() {
	if ImportPath == nil || *ImportPath == "" {
		log.Println("An export path not specified. Importing all data.")
		ImportAll = true
		return
	}
	str := strings.Split(*ImportPath, ".")
	if len(str) == 2 {
		TableToImport = str[1]
	}
	DBToImport = str[0]
}
