package _import

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/segmentio/encoding/json"

	"github.com/BOOMfinity/GoThink/pkg"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type databaseImport struct {
	name          string
	conn          *r.Session
	tables        []string
	databases     []string
	reader        *bufio.Reader
	workers       *workerPool
	dst           string
	hooksDisabled bool
}

func newDatabaseImport(name string, conn *r.Session, pool *workerPool) *databaseImport {
	di := databaseImport{
		name:          name,
		conn:          conn,
		workers:       pool,
		reader:        bufio.NewReader(bytes.NewBuffer(nil)),
		hooksDisabled: false,
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

// prepare rethink for importing data from tables
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

func (i *databaseImport) importTableInfo(name string, info *tar.Reader, shards, replicas uint64) error {
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
		_, err := r.DB(i.name).TableCreate(name, r.TableCreateOpts{
			PrimaryKey: tableInfo.PrimaryKey,
			Replicas:   replicas,
			Shards:     shards,
		}).Run(i.conn)
		if err != nil {
			return err
		}
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

	if tableInfo.WriteHook != nil && !i.hooksDisabled {
		/*
			Couldn't find any better way lol.

			From ql2.proto:
			SET_WRITE_HOOK = 189
			DB = 14;
			TABLE = 15;
		*/
		_, _ = r.RawQuery(json.RawMessage(fmt.Sprintf("[189,[[15,[[14,[\"%v\"]],\"%v\"]],{\"$reql_type$\":\"BINARY\",\"data\":\"%v\"}]]", i.name, name, base64.StdEncoding.EncodeToString(tableInfo.WriteHook.Function)))).Run(i.conn)
	}

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

var insertOpts = r.InsertOpts{
	IgnoreWriteHook: true,
	ReturnChanges:   false,
}

func (i *databaseImport) importTableChunk(name string, chunk *tar.Reader) error {
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
			_, err := r.DB(i.name).Table(name).Insert(p.GetTerms(), insertOpts).Run(i.conn)
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

// no-copy []byte -> string conversion.
func bytesToString(bytes []byte) (s string) {
	return *(*string)(unsafe.Pointer(&bytes))
}
