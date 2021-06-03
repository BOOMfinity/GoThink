package _import

import (
	"archive/tar"
	"io"
	"log"
	"os"
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
		workers = newWorkerPool()
		start   = time.Now()
		data    = ParseImportPath(importPath)
	)

	workers.Spawn(0)
	if err := ImportFile(filePath, DB, workers, data); err != nil {
		return err
	}

	println()
	log.Printf("Imported in %v", time.Since(start).String())
	println()

	return nil
}

// ImportFile streams data from backup file and processes it without fully unpacking to temp directories
func ImportFile(filePath string, conn *rethinkdb.Session, workers *workerPool, toImport pkg.ToExport) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder, _ := gzip.NewReader(file)
	reader := tar.NewReader(decoder)
	var ver *version.Version
	var currentImport *databaseImport
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
				log.Fatalf("This version of GoThink (%v) does NOT support backups from GoThink v%v. To continue, please download the older version that supports this backup.", GoThink.Version, ver.String())
			}
			continue
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		data := parseTarFilePath(header.Name)
		if currentImport == nil || currentImport.name != data.database { // if current import struct cannot be used for this database
			if toImport.Database != "" && toImport.Database != data.database { // if user specified one database to export
				continue
			} else {
				currentImport = NewDatabaseImport(data.database, conn, workers)
			}
		}

		switch data.typ {
		case FileInfo:
			if toImport.Table != "" && toImport.Table != data.table {
				break
			}
			err := currentImport.importTableInfo(data.table, reader)
			if err != nil {
				println("Failure during importing table info - backup file may be corrupted or versions mismatched.")
				panic(err)
			}
		case FileChunk:
			if toImport.Table != "" && toImport.Table != data.table {
				break
			}
			err := currentImport.importTableChunk(data.table, reader, data.chunkID)
			if err != nil {
				println("Failure during importing table chunk - backup file may be corrupted or versions mismatched.")
				panic(err)
			}
		}
	}
	return nil
}

// ParseImportPath parses selection of importable data from -i flag
func ParseImportPath(path string) (res pkg.ToExport) {
	if path == "" {
		log.Println("Import path not specified. Importing all data.")
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

// TarFileInfo represents information about data in current file in tar archive
type TarFileInfo struct {
	database string
	table    string
	typ      TarFileType
	chunkID  string // if typ == FileChunk
}

type TarFileType uint8

const (
	FileChunk = 0
	FileInfo  = 1
)

// parseTarFilePath extracts TarFileInfo from path of file in tar archive
func parseTarFilePath(path string) *TarFileInfo {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return nil
	}
	fi := &TarFileInfo{database: parts[0], table: parts[1]}
	switch {
	case strings.HasPrefix(parts[2], ".info"):
		fi.typ = FileInfo
	case strings.HasPrefix(parts[2], "chunk"):
		fi.typ = FileChunk
		fi.chunkID = parts[2][6 : len(parts[2])-5]
	default:
		return nil
	}
	return fi
}
