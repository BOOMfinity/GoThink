package pkg

import (
	"github.com/VenomPCPL/rethinkdb-go"
	"github.com/segmentio/encoding/json"
)

type ToExport struct {
	Table    string
	Database string
	All      bool
}

/*type TableDumpResult struct {
	totalSize      uint64
	totalDocuments uint64
}*/

type TableList map[string][]string

func (tl TableList) TotalCount() (total uint) {
	for _, table := range tl {
		total += uint(len(table))
	}
	return
}

type TableIndex struct {
	Multi    bool        `json:"multi"`
	Geo      bool        `json:"geo"`
	Index    string      `json:"index"`
	Function interface{} `json:"function"`
}

type TableWriteHook struct {
	Function interface{} `json:"function"`
}

type TableInfo struct {
	PrimaryKey     string                   `json:"primary_key"`
	Indexes        []TableIndex             `json:"indexes"`
	WriteHook      *rethinkdb.WriteHookInfo `json:"write_hooks"`
	TotalDocuments uint64                   `json:"total_documents"`
	TotalSize      uint64                   `json:"total_size"`
}

func (ti TableInfo) ToJSON() (res []byte) {
	res, _ = json.Marshal(ti)
	return
}
