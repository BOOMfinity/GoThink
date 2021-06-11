package _import

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/segmentio/encoding/json"

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
	var databases []string
	err := r.DBList().ReadAll(&databases, i.conn)
	if err != nil {
		return err
	}
	if !checkDatabase(i.databases, i.name) {
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
		r.DB(i.name).TableCreate(name, r.TableCreateOpts{
			PrimaryKey: tableInfo.PrimaryKey,
		}).Run(i.conn)
		r.DB(i.name).Table(name).Wait(r.WaitOpts{
			WaitFor: name,
		}).Run(i.conn)
	}

	for _, index := range tableInfo.Indexes {
		_, _ = r.DB(i.name).Table(name).IndexCreateFunc(index.Index, index.Function, r.IndexCreateOpts{
			Geo:   index.Geo,
			Multi: index.Multi,
		}).Run(i.conn)
	}
	r.DB(i.name).Table(name).IndexWait().Run(i.conn)
	bar1.SetTotal(int(tableInfo.TotalSize))
	bar1.Set(0)
	// TODO: Write hook import
	//if tableInfo.WriteHook != nil {
	//	_, _ = r.DB(i.name).Table(name).SetWriteHook(tableInfo.WriteHook.Function).Run(i.conn)
	//}

	return nil
}

var lu uint32
var l [4]byte

type insertDataSlice []insertData

func (i insertDataSlice) GetTerms() (p []interface{}) {
	for index := range i {
		p = append(p, i[index].val)
	}
	return
}

func (i insertDataSlice) GetSize() (p int) {
	for index := range i {
		p += i[index].size
	}
	return
}

type insertData struct {
	size int
	val  r.Term
}

func (i *databaseImport) importTableChunk(name string, chunk *tar.Reader, id string) error {
	bar1.Prefix(fmt.Sprintf("%v.%v ", i.name, name))
	var toInsert insertDataSlice
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
		jsonBytes := make([]byte, lu)
		_, err = io.ReadFull(chunk, jsonBytes)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		toInsert = append(toInsert, insertData{size: len(jsonBytes), val: r.JSON(bytesToString(jsonBytes))})
	}

	// Insert data in chunks of 250 elements in parallel using workers
	for _, dta := range chunkSlice(toInsert, 250) {
		i.workers.AddJob(func(x interface{}) {
			p := x.(insertDataSlice)
			_, err := r.DB(i.name).Table(name).Insert(p.GetTerms()).Run(i.conn)
			if err != nil {
				panic(err)
			}
			bar1.Add(p.GetSize())
		}, dta)
	}
	i.workers.Wait()

	return nil
}

// chunkSlice splits array into chunks of given length
func chunkSlice(xs insertDataSlice, chunkSize int) []insertDataSlice {
	if len(xs) == 0 {
		return nil
	}
	divided := make([]insertDataSlice, (len(xs)+chunkSize-1)/chunkSize)
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

// no-copy []byte -> string conversion. If you know better way to do this, feel free to tell us.
func bytesToString(bytes []byte) (s string) {
	return *(*string)(unsafe.Pointer(&bytes))
}
