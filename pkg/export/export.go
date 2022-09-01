package export

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/VenomPCPL/rethinkdb-go"
	"github.com/alitto/pond"
	"github.com/urfave/cli/v2"
	"go.uber.org/atomic"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/BOOMfinity/GoThink"
	"github.com/BOOMfinity/GoThink/pkg"
	"github.com/cheggaaa/pb"
	"github.com/klauspost/pgzip"
)

func RunFromCLI(ctx *cli.Context) error {
	return Run(ctx.Context.Value("database").(*rethinkdb.Session), ctx.String("export-path"), ctx.String("output"))
}

func Run(DB *rethinkdb.Session, exportPath, outputPath string) error {
	tempDir, err := os.MkdirTemp(os.TempDir(), "gothink.export.*")
	if err != nil {
		return err
	}
	defer func() {
		if _err := recover(); _err != nil {
			_ = os.RemoveAll(tempDir)
			panic(_err)
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
		rows    = atomic.NewUint64(0)
		barPool = InitBars(bar1, bar2)
		now     = time.Now()
		l       = make([]byte, 4)
		workers = pond.New(runtime.GOMAXPROCS(0), 0)
	)
	barPool.Start()
	for _, db := range dbs {
		bar1.Increment()
		bar1.Prefix(fmt.Sprintf("Exporting '%v'", db))
		tables := tableMap[db]
		bar2.SetTotal(len(tables))
		bar2.Set(0)
		bar2.Prefix("Working...")
		workerGroup := workers.Group()
		for _, _table := range tables {
			table := strings.Clone(_table)
			println(table)
			workerGroup.Submit(func() {
				buff := new(bytes.Buffer)
				buff.Grow(31457280)
				// Documents
				var (
					totalSize      = uint64(0)
					totalDocuments = uint64(0)
				)

				_err := os.MkdirAll(filepath.Join(tempDir, fmt.Sprintf("%v/%v", db, table)), 0700)
				if _err != nil {
					panic(_err)
				}
				var tableInfo map[string]interface{}
				rethinkdb.DB(db).Table(table).Info().ReadOne(&tableInfo, DB)
				bar2.Increment()
				buff.Reset()
				chunkID := 0
				_err = forEachTable(DB, db, table, func(data []byte) error {
					rows.Add(1)
					binary.BigEndian.PutUint32(l[0:4], uint32(len(data)))
					buff.Write(l)
					buff.Write(data)
					totalSize += uint64(len(data))
					totalDocuments++
					if buff.Len() >= 26214400 { // 25MiB
						if __err := writeFile(tempDir, db, table, chunkID, buff); err != nil {
							return __err
						}
						chunkID++
						buff.Reset()
						return nil
					}
					return nil
				})
				if _err != nil {
					panic(_err)
				}
				if _err = writeFile(tempDir, db, table, chunkID, buff); _err != nil {
					panic(_err)
				}

				// Secondary indexes
				var (
					allIndexes []pkg.TableIndex
					whook      *rethinkdb.WriteHookInfo
				)
				rethinkdb.DB(db).Table(table).IndexStatus().ReadAll(&allIndexes, DB, rethinkdb.RunOpts{BinaryFormat: "raw"})
				rethinkdb.DB(db).Table(table).GetWriteHook().ReadOne(&whook, DB)
				info := pkg.TableInfo{
					PrimaryKey:     tableInfo["primary_key"].(string),
					Indexes:        allIndexes,
					WriteHook:      whook,
					TotalDocuments: totalDocuments,
					TotalSize:      totalSize,
				}
				_err = os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("%v/%v/.info.json", db, table)), info.ToJSON(), 0600)
				if _err != nil {
					panic(_err)
				}
			})
		}
		workerGroup.Wait()
	}

	bar1.Prefix("Waiting...")
	bar2.Prefix("Waiting...")
	if err = os.WriteFile(filepath.Join(tempDir, ".version"), []byte(GoThink.Version), 0600); err != nil {
		return err
	}

	// Zipping
	bar1.Set(0)
	bar2.Set(0)
	os.MkdirAll(filepath.Dir(outputPath), 0700)
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
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
	err = os.RemoveAll(tempDir)
	if err != nil {
		log.Printf("Temp dir has not been removed due to error: %v\nPlease do it yourself. (%v)", err.Error(), tempDir)
	}
	bar1.Prefix("Finished!")
	bar2.Prefix("Finished!")
	barPool.Stop()
	end := time.Now()
	println()
	log.Printf("%v documents exported in %v", rows.Load(), end.Sub(now).String())
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
	err := os.WriteFile(filepath.Join(temp, fmt.Sprintf("%v/%v/chunk-%v.json", db, table, chunk)), buff.Bytes(), 0600)
	return err
}

func forEachTable(DB *rethinkdb.Session, db, table string, run func(data []byte) error) error {
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
