package _import

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"unsafe"

	"github.com/BOOMfinity-Developers/GoThink/pkg"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type databaseImport struct {
	name      string
	conn      *r.Session
	tables    []string
	databases []string
	reader    *bufio.Reader
	workers   *workerPool
	dst       string
}

func NewDatabaseImport(name string, conn *r.Session, pool *workerPool, databases []string) *databaseImport {
	return &databaseImport{
		name:      name,
		conn:      conn,
		workers:   pool,
		databases: databases,
		reader:    bufio.NewReader(bytes.NewBuffer(nil)),
	}
}

func (i *databaseImport) SetDestination(path string) {
	i.dst = path
}

func (i *databaseImport) Run() error {
	if err := i.importDatabase(); err != nil {
		return err
	}
	println()
	return nil
}

func (i *databaseImport) importDatabase() error {
	if !checkDatabase(i.databases, i.name) {
		log.Printf("Creating '%v'...", i.name)
		r.DBCreate(i.name).Run(i.conn)
	}
	r.DB(i.name).TableList().ReadAll(&i.tables, i.conn)
	tables, err := os.ReadDir(filepath.Join(i.dst, i.name))
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
	var tableInfo pkg.TableInfo
	infoFile, _ := os.Open(filepath.Join(i.dst, i.name, name, "info.json"))
	if err := parseFile(&tableInfo, infoFile); err != nil {
		return err
	}
	if !i.tableExist(name) {
		log.Printf("Creating table '%v' in '%v'...", name, i.name)
		r.DB(i.name).TableCreate(name, r.TableCreateOpts{
			PrimaryKey: tableInfo.PrimaryKey,
		}).Run(i.conn)
		r.DB(i.name).Table(name).Wait(r.WaitOpts{
			WaitFor: name,
		}).Run(i.conn)
	}
	log.Printf("Importing indexes...")
	for _, index := range tableInfo.Indexes {
		_, _ = r.DB(i.name).Table(name).IndexCreateFunc(index.Index, index.Function, r.IndexCreateOpts{
			Geo:   index.Geo,
			Multi: index.Multi,
		}).Run(i.conn)
	}
	r.DB(i.name).Table(name).IndexWait().Run(i.conn)
	log.Printf("Importing documents...")
	chunks, _ := os.ReadDir(filepath.Join(i.dst, i.name, name))
	for _, chunk := range chunks {
		if chunk.Name() == "info.json" {
			continue
		}
		chunkFile, err := os.Open(filepath.Join(i.dst, i.name, name, chunk.Name()))
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
			i.workers.AddJob(func(x interface{}) {
				_, err = r.DB(i.name).Table(name).Insert(x.([]interface{})).Run(i.conn)
				if err != nil {
					panic(err)
				}
			}, dta)
		}
		i.workers.Wait()
		// TODO: Write hook import
		//if tableInfo.WriteHook != nil {
		//	_, _ = r.DB(i.name).Table(name).SetWriteHook(tableInfo.WriteHook.Function).Run(i.conn)
		//}
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

func checkDatabase(dbs []string, name string) bool {
	for _, db := range dbs {
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
