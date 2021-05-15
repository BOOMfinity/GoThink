package main

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BOOMfinity-Developers/GoThink"
	"github.com/cheggaaa/pb"
	"github.com/jessevdk/go-flags"
	"github.com/klauspost/pgzip"
	"github.com/segmentio/encoding/json"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"

	"github.com/BOOMfinity-Developers/GoThink/database"
)

var (
	ExportAll     = false
	TableToExport = ""
	DBToExport    = ""
	bar1          = pb.New(0).SetMaxWidth(100)
	bar2          = pb.New(0).SetMaxWidth(100)
	Flags         GoThink.ExportFlags
	parser        = flags.NewNamedParser("gothink-export", flags.Default)
)

func init() {
	println()
	println("Welcome to RethinkGO-Backups CLI v" + GoThink.Version)
	println()
	parser.AddGroup("Export", "", &Flags)
	database.AddFlags(parser)
	_, err := parser.Parse()
	var parserError *flags.Error
	if errors.As(err, &parserError) {
		if parserError.Type == flags.ErrHelp {
			os.Exit(0)
		}
		panic(err)
	}
	if err != nil {
		panic(err)
	}
}

func main() {
	c, err := database.NewConnection()
	if err != nil {
		panic(err)
	}
	println()
	parseExportPath(c)
	if DBToExport != "" || TableToExport != "" {
		if DBToExport != "" {
			log.Printf("DB to export: %v", DBToExport)
		} else {
			log.Printf("DB to export: (not set)")
		}
		if TableToExport != "" {
			log.Printf("Table to export: %v", TableToExport)
		} else {
			log.Printf("Table to export: (not set)")
		}
	}
	println()
	var (
		dbs      GoThink.PowerfulStringSlice
		tableMap = make(database.TableList)
	)
	if ExportAll {
		if err := r.DBList().ReadAll(&dbs, c.DB); err != nil {
			panic(err)
		}
		dbs = dbs.Filter(func(a string) bool {
			return a != "rethinkdb"
		})
	} else {
		dbs = GoThink.PowerfulStringSlice{DBToExport}
	}
	if TableToExport == "" {
		for _, db := range dbs {
			var tables []string
			if err := r.DB(db).TableList().ReadAll(&tables, c.DB); err != nil {
				panic(err)
			}
			tableMap[db] = tables
		}
	} else {
		tableMap[DBToExport] = GoThink.PowerfulStringSlice{TableToExport}
	}
	log.Printf("Exporting %v databases (%v tables)...", len(dbs), tableMap.TotalCount())
	println()
	bar1.SetTotal(len(dbs))
	bar1.ShowElapsedTime = false
	bar2.ShowElapsedTime = false
	bar1.ShowFinalTime = false
	bar2.ShowFinalTime = false
	bar1.ShowSpeed = false
	bar2.ShowSpeed = false
	bar1.ShowTimeLeft = false
	bar2.ShowTimeLeft = false
	bar1.Prefix("Waiting...")
	bar2.Prefix("Waiting...")
	pool, _ := pb.StartPool(bar1, bar2)
	buff := new(bytes.Buffer)
	var rows uint64
	now := time.Now()
	bar1.Set(0)
	tempDir, err := ioutil.TempDir(os.TempDir(), "gothink.export.*")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := recover(); err != nil {
			_ = os.RemoveAll(tempDir)
			panic(err)
		}
	}()
	var (
		totalSize      uint64
		totalDocuments uint64
	)
	for _, db := range dbs {
		bar1.Increment()
		bar1.Prefix(fmt.Sprintf("Exporting '%v'", db))
		tables := tableMap[db]
		bar2.SetTotal(len(tables))
		bar2.Set(0)
		for _, table := range tables {
			totalSize = 0
			totalDocuments = 0
			os.MkdirAll(filepath.Join(tempDir, fmt.Sprintf("%v/%v", db, table)), 0755)
			var tableInfo map[string]interface{}
			r.DB(db).Table(table).Info().ReadOne(&tableInfo, c.DB)
			bar2.Prefix(fmt.Sprintf("Table '%v'", table))
			bar2.Increment()
			buff.Reset()
			chunkID := 0
			msg := make(chan interface{})
			cursor, _ := r.DB(db).Table(table, r.TableOpts{ReadMode: "outdated"}).Run(c.DB)
			cursor.Listen(msg)
			for data := range msg {
				rows++
				dataJ, _ := json.Marshal(data)
				var l = make([]byte, 4)
				binary.BigEndian.PutUint32(l[0:4], uint32(len(dataJ)))
				buff.Write(l)
				buff.Write(dataJ)
				totalSize += uint64(len(dataJ))
				totalDocuments++
				if buff.Len() >= /*5242880*/ 26214400 { // 25MiB
					err = os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("%v/%v/chunk-%v.json", db, table, chunkID)), buff.Bytes(), 0755)
					if err != nil {
						panic(err)
					}
					chunkID++
					buff.Reset()
					continue
				}
			}
			err = os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("%v/%v/chunk-%v.json", db, table, chunkID)), buff.Bytes(), 0755)
			if err != nil {
				panic(err)
			}

			// Secondary indexes
			var (
				allIndexes []database.TableIndex
				whook      *database.TableWriteHook
			)

			r.DB(db).Table(table).IndexStatus().ReadAll(&allIndexes, c.DB, r.RunOpts{BinaryFormat: "raw"})
			r.DB(db).Table(table).GetWriteHook().ReadOne(&whook, c.DB, r.RunOpts{BinaryFormat: "raw"})
			info := database.TableInfo{
				PrimaryKey:     tableInfo["primary_key"].(string),
				Indexes:        allIndexes,
				WriteHook:      whook,
				TotalDocuments: totalDocuments,
				TotalSize:      totalSize,
			}
			err = os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("%v/%v/info.json", db, table)), info.ToJSON(), 0755)
			if err != nil {
				panic(err)
			}
		}
	}

	bar1.Prefix("Waiting...")
	bar2.Prefix("Waiting...")

	err = os.WriteFile(filepath.Join(tempDir, ".version"), []byte(GoThink.Version), 0755)

	// tar.gz
	bar1.Set(0)
	bar2.Set(0)
	os.MkdirAll(filepath.Dir(Flags.File), 0755)
	file, err := os.OpenFile(Flags.File, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		panic(err)
	}
	zWriter, _ := pgzip.NewWriterLevel(file, pgzip.BestCompression)
	tWriter := tar.NewWriter(zWriter)

	dirs, err := os.ReadDir(tempDir)
	if err != nil {
		panic(err)
	}
	bar1.SetTotal(len(dirs))
	bar2.SetTotal(0)
	bar2.Set(0)
	bar2.Prefix("Waiting...")
	for _, dir := range dirs {
		bar1.Prefix(fmt.Sprintf("Packing '%v'", dir.Name()))
		bar1.Increment()
		check(filepath.Join(tempDir, dir.Name()), tWriter, dir.Name())
	}

	tWriter.Close()
	zWriter.Close()
	file.Close()
	os.RemoveAll(tempDir)
	bar1.Prefix("Finished!")
	bar2.Prefix("Finished!")
	pool.Stop()
	end := time.Now()
	println()
	log.Printf("%v documents exported in %v", rows, end.Sub(now).String())
	println()
}

func check(path string, tW *tar.Writer, fixedPath string) {
	file, _ := os.Open(path)
	info, _ := file.Stat()

	header, _ := tar.FileInfoHeader(info, info.Name())
	header.Name = fixedPath
	tW.WriteHeader(header)
	if info.IsDir() {
		files, _ := os.ReadDir(path)
		for _, dirFile := range files {
			check(filepath.Join(path, dirFile.Name()), tW, filepath.Join(fixedPath, dirFile.Name()))
		}
		return
	}
	io.Copy(tW, file)
	file.Close()
}

func parseExportPath(conn *database.Connection) {
	if Flags.Path == "" {
		log.Println("An export path not specified. Exporting all data.")
		ExportAll = true
		return
	}
	str := strings.Split(Flags.Path, ".")
	if len(str) == 2 {
		TableToExport = str[1]
	}
	DBToExport = str[0]
	var (
		dbs    GoThink.PowerfulStringSlice
		tables GoThink.PowerfulStringSlice
	)
	r.DBList().ReadAll(&dbs, conn.DB)
	if !(func() bool {
		for _, db := range dbs {
			if db == DBToExport {
				return true
			}
		}
		return false
	})() {
		log.Fatal("Database not found")
	}
	r.DB(DBToExport).TableList().ReadAll(&tables, conn.DB)
	if TableToExport != "" && !(func() bool {
		for _, t := range tables {
			if t == TableToExport {
				return true
			}
		}
		return false
	})() {
		log.Fatal("Table not found")
	}
}
