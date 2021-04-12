package database

import "encoding/json"

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

type TableInfo struct {
	PrimaryKey string                   `json:"primary_key"`
	Indexes    []TableIndex             `json:"indexes"`
	WriteHooks []map[string]interface{} `json:"write_hooks"`
}

func (ti TableInfo) ToJSON() (res []byte) {
	res, _ = json.Marshal(ti)
	return
}
