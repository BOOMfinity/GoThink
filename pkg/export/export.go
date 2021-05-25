package export

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
	"github.com/BOOMfinity-Developers/GoThink/pkg"
	"github.com/cheggaaa/pb"
	"github.com/klauspost/pgzip"
	"github.com/urfave/cli/v2"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func RunFromCLI(ctx *cli.Context) error {
	return Run(ctx.Context.Value("database").(*rethinkdb.Session), ctx.String("export-path"), ctx.String("output"))
}

func Run(DB *rethinkdb.Session, exportPath, outputPath string) error {
	defer func() {
		if err := recover(); err != nil {
			panic(err)
		}
	}()
	data, err := ParseExportPath(DB, exportPath)
	if err != nil {
		return err
	}
	if data.Database != "" || data.Table != "" {
		if data.Database != "" {
			log.Printf("DB to export: %v", data.Database)
		} else {
			log.Printf("DB to export: (not set)")
		}
		if data.Table != "" {
			log.Printf("Table to export: %v", data.Table)
		} else {
			log.Printf("Table to export: (not set)")
		}
	}
	println()
	var (
		dbs      GoThink.PowerfulStringSlice
		tableMap = make(pkg.TableList)
		bar1     = pb.New(0).SetMaxWidth(100)
		bar2     = pb.New(0).SetMaxWidth(100)
	)
	if data.All {
		if err := rethinkdb.DBList().ReadAll(&dbs, DB); err != nil {
			panic(err)
		}
		dbs = dbs.Filter(func(a string) bool {
			return a != "rethinkdb"
		})
	} else {
		dbs = GoThink.PowerfulStringSlice{data.Database}
	}
	if data.Table == "" {
		for _, db := range dbs {
			var tables []string
			if err := rethinkdb.DB(db).TableList().ReadAll(&tables, DB); err != nil {
				panic(err)
			}
			tableMap[db] = tables
		}
	} else {
		tableMap[data.Database] = GoThink.PowerfulStringSlice{data.Table}
	}
	log.Printf("Exporting %v databases (%v tables)...", len(dbs), tableMap.TotalCount())
	println()
	bar1.SetTotal(len(dbs))
	var (
		buff       = new(bytes.Buffer)
		rows       = 0
		barPool    = InitBars(bar1, bar2)
		now        = time.Now()
		tempDir, _ = ioutil.TempDir(os.TempDir(), "gothink.export.*")
		l          = make([]byte, 4)
	)
	buff.Grow(31457280)
	barPool.Start()
	for _, db := range dbs {
		bar1.Increment()
		bar1.Prefix(fmt.Sprintf("Exporting '%v'", db))
		tables := tableMap[db]
		bar2.SetTotal(len(tables))
		bar2.Set(0)
		for _, table := range tables {
			// Documents
			var (
				totalSize      = uint64(0)
				totalDocuments = uint64(0)
			)
			os.MkdirAll(filepath.Join(tempDir, fmt.Sprintf("%v/%v", db, table)), 0755)
			var tableInfo map[string]interface{}
			rethinkdb.DB(db).Table(table).Info().ReadOne(&tableInfo, DB)
			bar2.Prefix(fmt.Sprintf("Table '%v'", table))
			bar2.Increment()
			buff.Reset()
			chunkID := 0
			err = ForEachTables(DB, db, table, func(data []byte) error {
				rows++
				binary.BigEndian.PutUint32(l[0:4], uint32(len(data)))
				buff.Write(l)
				buff.Write(data)
				totalSize += uint64(len(data))
				totalDocuments++
				if buff.Len() >= /*5242880*/ 26214400 { // 25MiB
					if err := writeFile(tempDir, db, table, chunkID, buff); err != nil {
						return err
					}
					chunkID++
					buff.Reset()
					return nil
				}
				return nil
			})
			if err != nil {
				return err
			}
			if err := writeFile(tempDir, db, table, chunkID, buff); err != nil {
				return err
			}

			// Secondary indexes
			var (
				allIndexes []pkg.TableIndex
				whook      *pkg.TableWriteHook
			)
			rethinkdb.DB(db).Table(table).IndexStatus().ReadAll(&allIndexes, DB, rethinkdb.RunOpts{BinaryFormat: "raw"})
			rethinkdb.DB(db).Table(table).GetWriteHook().ReadOne(&whook, DB, rethinkdb.RunOpts{BinaryFormat: "raw"})
			info := pkg.TableInfo{
				PrimaryKey:     tableInfo["primary_key"].(string),
				Indexes:        allIndexes,
				WriteHook:      whook,
				TotalDocuments: totalDocuments,
				TotalSize:      totalSize,
			}
			err = os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("%v/%v/.info.json", db, table)), info.ToJSON(), 0755)
			if err != nil {
				panic(err)
			}
		}
	}

	bar1.Prefix("Waiting...")
	bar2.Prefix("Waiting...")
	if err = os.WriteFile(filepath.Join(tempDir, ".version"), []byte(GoThink.Version), 0755); err != nil {
		return err
	}

	// Zipping
	bar1.Set(0)
	bar2.Set(0)
	os.MkdirAll(filepath.Dir(outputPath), 0755)
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
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
	barPool.Stop()
	end := time.Now()
	println()
	log.Printf("%v documents exported in %v", rows, end.Sub(now).String())
	println()
	return nil
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

func writeFile(temp, db, table string, chunk int, buff *bytes.Buffer) error {
	err := os.WriteFile(filepath.Join(temp, fmt.Sprintf("%v/%v/chunk-%v.json", db, table, chunk)), buff.Bytes(), 0755)
	return err
}

func ForEachTables(DB *rethinkdb.Session, db, table string, run func(data []byte) error) error {
	cursor, _ := rethinkdb.DB(db).Table(table, rethinkdb.TableOpts{ReadMode: "outdated"}).Run(DB)
	for {
		data, ok := cursor.NextResponse()
		if !ok {
			break
		}
		if err := run(data); err != nil {
			return err
		}
	}
	return nil
}

func ParseExportPath(db *rethinkdb.Session, path string) (ex pkg.ToExport, err error) {
	if path == "" {
		log.Println("Export path not specified - exporting all data!")
		ex.All = true
		return
	}
	str := strings.Split(path, ".")
	if len(str) == 2 {
		ex.Table = str[1]
	}
	ex.Database = str[0]
	var (
		dbs    GoThink.PowerfulStringSlice
		tables GoThink.PowerfulStringSlice
	)
	rethinkdb.DBList().ReadAll(&dbs, db)
	if !(func() bool {
		for _, db := range dbs {
			if db == ex.Database {
				return true
			}
		}
		return false
	})() {
		err = errors.New("database not found")
		return
	}
	rethinkdb.DB(ex.Database).TableList().ReadAll(&tables, db)
	if ex.Table != "" && !(func() bool {
		for _, t := range tables {
			if t == ex.Table {
				return true
			}
		}
		return false
	})() {
		err = errors.New("table not found")
		return
	}
	return
}

func InitBars(bars ...*pb.ProgressBar) *pb.Pool {
	for _, bar := range bars {
		bar.ShowElapsedTime = false
		bar.ShowFinalTime = false
		bar.ShowSpeed = false
		bar.ShowTimeLeft = false
		bar.Prefix("Waiting...")
	}
	return pb.NewPool(bars...)
}
