package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rethinkgo-backups/database"
	"strings"
	"time"
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
	println("Welcome to RethinkGO-Backups CLI")
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

	workers.Spawn(0)

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