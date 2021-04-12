package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cheggaaa/pb"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"io"
	"log"
	"os"
	"path/filepath"
	rethinkgo "rethinkgo-backups"
	"rethinkgo-backups/database"
	"strings"
	"time"
)

var (
	ExportPath = flag.String("export", "", "What will be exported. Use a database.table syntax.")
	ExportAll = false
	TableToExport = ""
	DBToExport = ""
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
	os.RemoveAll(".backups/")
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
		dbs rethinkgo.PowerfulStringSlice
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
		dbs = rethinkgo.PowerfulStringSlice{DBToExport}
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
		tableMap[DBToExport] = rethinkgo.PowerfulStringSlice{TableToExport}
	}
	log.Printf("Exporting %v databases (%v tables)...", len(dbs), tableMap.TotalCount())
	println()
	bar1 := pb.New(len(dbs)).SetMaxWidth(100)
	bar2 := pb.New(0).SetMaxWidth(100)
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
	var file *os.File
	var rows uint64
	now := time.Now()
	for _, db := range dbs {
		bar1.Increment()
		bar1.Prefix(fmt.Sprintf("Exporting '%v'", db))
		tables := tableMap[db]
		bar2.SetTotal(len(tables))
		bar2.Set(0)
		for _, table := range tables {
			var tableInfo map[string]interface{}
			r.DB(db).Table(table).Info().ReadOne(&tableInfo, c.DB)
			bar2.Increment()
			buff.Reset()
			chunkID := 0
			msg := make(chan interface{})
			os.MkdirAll(".backups/"+db+"/"+table+"/", os.FileMode(os.O_CREATE))
			bar2.Prefix(fmt.Sprintf("Table '%v'", table))
			file, _ = os.Create(fmt.Sprintf(".backups/%v/%v/chunk-%v.json", db, table, chunkID))
			cursor, _ := r.DB(db).Table(table, r.TableOpts{ ReadMode: "outdated" }).Run(c.DB)
			cursor.Listen(msg)
			for data := range msg {
				rows++
				dataJ, _ := json.Marshal(data)
				buff.Write(dataJ)
				buff.WriteString("\n")
				if buff.Len() >= 26214400 { // 25MiB
					file.Write(buff.Bytes())
					file.Close()
					chunkID++
					buff.Reset()
					file, _ = os.Create(fmt.Sprintf(".backups/%v/%v/chunk-%v.json", db, table, chunkID))
					continue
				}
			}
			file.Write(buff.Bytes())
			file.Close()

			// Secondary indexes
			var allIndexes []database.TableIndex
			r.DB(db).Table(table).IndexStatus().ReadAll(&allIndexes, c.DB, r.RunOpts{ BinaryFormat: "raw" })
			iFile, _ := os.Create(fmt.Sprintf(".backups/%v/%v/info.json", db, table))
			info := database.TableInfo {
				PrimaryKey: tableInfo["primary_key"].(string),
				Indexes: allIndexes,
			}
			iFile.Write(info.ToJSON())
			iFile.Close()
		}
	}
	end := time.Now()
	bar1.Prefix("Waiting...")
	bar2.Prefix("Waiting...")
	time.Sleep(0 * time.Second)

	// tar.gz
	bar1.Set(0)
	bar2.Set(0)
	file, err = os.Create("backup.tar.gz")
	if err != nil {
		panic(err)
	}
	//zWriter, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedBestCompression),
	zWriter := gzip.NewWriter(file)
	//if err != nil {
	//	panic(err)
	//}
	tWriter := tar.NewWriter(zWriter)

	dirs, _ := os.ReadDir(".backups")
	for _, dir := range dirs {
		check(filepath.Join(".backups", dir.Name()), tWriter)
	}

	tWriter.Close()
	zWriter.Close()
	file.Close()
	os.RemoveAll(".backups/")
	bar1.Prefix("Finished!")
	bar2.Prefix("Finished!")
	time.Sleep(0 * time.Second)
	pool.Stop()
	println()
	log.Printf("%v rows exported in %v", rows, end.Sub(now).String())
	println()
}

func check(path string, tW *tar.Writer) {
	file, _ := os.Open(path)
	info, _ := file.Stat()

	header, _ := tar.FileInfoHeader(info, info.Name())
	header.Name = strings.ReplaceAll(strings.ReplaceAll(path, ".backups/", ""), ".backups\\", "")
	tW.WriteHeader(header)
	if info.IsDir() {
		files, _ := os.ReadDir(path)
		for _, dirFile := range files {
			check(filepath.Join(path, dirFile.Name()), tW)
		}
		return
	}
	io.Copy(tW, file)
	file.Close()
}

func parseExportPath(conn *database.Connection) {
	if ExportPath == nil || *ExportPath == "" {
		log.Println("An export path not specified. Exporting all data.")
		ExportAll = true
		return
	}
	str := strings.Split(*ExportPath, ".")
	if len(str) == 2 {
		TableToExport = str[1]
	}
	DBToExport = str[0]
	var (
		dbs rethinkgo.PowerfulStringSlice
		tables rethinkgo.PowerfulStringSlice
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
