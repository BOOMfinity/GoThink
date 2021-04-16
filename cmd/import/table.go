package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"rethinkgo-backups/database"
	"unsafe"
)

type databaseImport struct {
	name   string
	conn   *database.Connection
	tables []string
	reader *bufio.Reader
}

func newDatabaseImport(name string, conn *database.Connection) *databaseImport {
	return &databaseImport{
		name:   name,
		conn:   conn,
		reader: bufio.NewReader(bytes.NewBuffer(nil)),
	}
}

func (i *databaseImport) Run() error {
	if err := i.importDatabase(); err != nil {
		return err
	}
	println()
	return nil
}

func (i *databaseImport) importDatabase() error {
	if !checkDatabase(i.name) {
		log.Printf("Creating '%v'...", i.name)
		r.DBCreate(i.name).Run(i.conn.DB)
	}
	r.DB(i.name).TableList().ReadAll(&i.tables, i.conn.DB)
	tables, err := os.ReadDir(filepath.Join(dst, i.name))
	if err != nil {
		return err
	}

	for _, table := range tables {
		if err = i.importTable(table.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (i *databaseImport) tableExist(name string) bool {
	for _, table := range i.tables {
		if table == name {
			return true
		}
	}
	return false
}

func (i *databaseImport) importTable(name string) error {
	var tableInfo database.TableInfo
	infoFile, _ := os.Open(filepath.Join(dst, i.name, name, "info.json"))
	parseFile(&tableInfo, infoFile)
	if !i.tableExist(name) {
		log.Printf("Creating table '%v' in '%v'...", name, i.name)
		r.DB(i.name).TableCreate(name, r.TableCreateOpts{
			PrimaryKey: tableInfo.PrimaryKey,
		}).Run(i.conn.DB)
		r.DB(i.name).Table(name).Wait(r.WaitOpts{
			WaitFor: name,
		}).Run(i.conn.DB)
	}
	log.Printf("Importing indexes...")
	for _, index := range tableInfo.Indexes {
		_, _ = r.DB(i.name).Table(name).IndexCreateFunc(index.Index, index.Function, r.IndexCreateOpts{
			Geo:   index.Geo,
			Multi: index.Multi,
		}).Run(i.conn.DB)
	}
	r.DB(i.name).Table(name).IndexWait().Run(i.conn.DB)
	log.Printf("Importing documents...")
	chunks, _ := os.ReadDir(filepath.Join(dst, i.name, name))
	for _, chunk := range chunks {
		if chunk.Name() == "info.json" {
			continue
		}
		chunkFile, err := os.Open(filepath.Join(dst, i.name, name, chunk.Name()))
		if err != nil {
			return err
		}
		i.reader.Reset(chunkFile)
		var lu uint32
		var l [4]byte
		var toInsert []interface{}
		for {
			_, err = io.ReadFull(i.reader, l[:])
			lu = binary.BigEndian.Uint32(l[0:4])
			if err != nil {
				if err != io.EOF {
					panic(err)
				}
				break
			}
			d := make([]byte, lu)
			_, err = io.ReadFull(i.reader, d)
			if err != nil {
				if err != io.EOF {
					panic(err)
				}
				break
			}

			sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&d))
			stringHeader := reflect.StringHeader{Data: sliceHeader.Data, Len: sliceHeader.Len}
			toInsert = append(toInsert, r.JSON(*(*string)(unsafe.Pointer(&stringHeader))))
		}
		for _, dta := range chunkSlice(toInsert, 250) {
			workers.AddJob(func(x interface{}) {
				_, err = r.DB(i.name).Table(name).Insert(x.([]interface{})).Run(i.conn.DB)
				if err != nil {
					panic(err)
				}
			}, dta)
		}
		workers.Wait()
		log.Println("Finished")
	}
	return nil
}

func chunkSlice(xs []interface{}, chunkSize int) [][]interface{} {
	if len(xs) == 0 {
		return nil
	}
	divided := make([][]interface{}, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
}

func checkDatabase(name string) bool {
	for _, db := range databases {
		if db == name {
			return true
		}
	}
	return false
}

func parseFile(dst interface{}, file io.Reader) error {
	if data, err := io.ReadAll(file); err != nil {
		return err
	} else if err = json.Unmarshal(data, dst); err != nil {
		return err
	}
	return nil
}
