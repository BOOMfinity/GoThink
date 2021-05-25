package _import

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/segmentio/encoding/json"
	"io"
	"log"
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

func NewDatabaseImport(name string, conn *r.Session, pool *workerPool) *databaseImport {
	di := databaseImport{
		name:    name,
		conn:    conn,
		workers: pool,
		reader:  bufio.NewReader(bytes.NewBuffer(nil)),
	}
	err := di.prepare()
	if err != nil {
		panic(err)
		return nil
	}
	return &di
}

func (i *databaseImport) SetDestination(path string) {
	i.dst = path
}

// prepare prepares rethink for importing data from tables
func (i *databaseImport) prepare() error {
	println("Importing ", i.name, " database")
	var databases []string
	err := r.DBList().ReadAll(&databases, i.conn)
	if err != nil {
		return err
	}
	if !checkDatabase(i.databases, i.name) {
		log.Printf("Creating '%v'...", i.name)
		r.DBCreate(i.name).Run(i.conn)
	}
	err = r.DB(i.name).TableList().ReadAll(&i.tables, i.conn)
	if err != nil {
		return err
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

func (i *databaseImport) importTableInfo(name string, info *tar.Reader) error {
	tableInfo := pkg.TableInfo{}
	jsonBytes, err := io.ReadAll(info)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsonBytes, &tableInfo)
	if err != nil {
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

	log.Printf("Importing indexes of %v...", name)
	for _, index := range tableInfo.Indexes {
		_, _ = r.DB(i.name).Table(name).IndexCreateFunc(index.Index, index.Function, r.IndexCreateOpts{
			Geo:   index.Geo,
			Multi: index.Multi,
		}).Run(i.conn)
	}
	r.DB(i.name).Table(name).IndexWait().Run(i.conn)

	// TODO: Write hook import
	//if tableInfo.WriteHook != nil {
	//	_, _ = r.DB(i.name).Table(name).SetWriteHook(tableInfo.WriteHook.Function).Run(i.conn)
	//}

	return nil
}

var lu uint32
var l [4]byte

func (i *databaseImport) importTableChunk(name string, chunk *tar.Reader, id string) error {
	var toInsert []interface{}
	for {
		// Read uint32 - length of next document
		_, err := io.ReadFull(chunk, l[:])
		lu = binary.BigEndian.Uint32(l[0:4])
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}

		// Read JSON document and prepare for inserting
		d := make([]byte, lu)
		_, err = io.ReadFull(chunk, d)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		// no-copy []byte -> string conversion
		sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&d))
		stringHeader := reflect.StringHeader{Data: sliceHeader.Data, Len: sliceHeader.Len}
		toInsert = append(toInsert, r.JSON(*(*string)(unsafe.Pointer(&stringHeader))))
	}

	// Insert data in chunks of 250 elements in parallel using workers
	for _, dta := range chunkSlice(toInsert, 250) {
		i.workers.AddJob(func(x interface{}) {
			_, err := r.DB(i.name).Table(name).Insert(x.([]interface{})).Run(i.conn)
			if err != nil {
				panic(err)
			}
		}, dta)
	}
	i.workers.Wait()

	log.Printf("Finished importing chunk %v-#%v\n", name, id)
	return nil
}

// chunkSlice splits array into chunks of given length
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
