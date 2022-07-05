package check

import (
	"fmt"
	"strings"
	"time"

	"github.com/BOOMfinity/GoThink"
	"github.com/segmentio/encoding/json"
	"github.com/urfave/cli/v2"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func RunFromCLI(ctx *cli.Context) error {
	return Run(ctx.Context.Value("database").(*rethinkdb.Session), ctx.Context.Value("database-b").(*rethinkdb.Session), ctx.String("database"))
}

func Run(srcDB, targetDB *rethinkdb.Session, toCheck string) error {
	checkAll := toCheck == ""
	var (
		dbs     GoThink.PowerfulStringSlice
		summary struct {
			missingDatabases    uint64
			missingTables       uint64
			missingDocuments    uint64
			mismatchedDocuments uint64
			errors              uint64
			totalDocuments      uint64
		}
	)
	if checkAll {
		rethinkdb.DBList().ReadAll(&dbs, srcDB)
		dbs = dbs.Filter(func(a string) bool {
			return a != "rethinkdb"
		})
	} else {
		dbs = GoThink.PowerfulStringSlice{toCheck}
	}
	println("Working...")
	now := time.Now()
	for _, db := range dbs {
		var (
			tables []string
		)
		if err := rethinkdb.DB(db).TableList().ReadAll(&tables, srcDB); err != nil {
			return err
		}
		for _, table := range tables {
			var info map[string]interface{}
			rethinkdb.DB(db).Table(table).Info().ReadOne(&info, srcDB)
			cursor, err := rethinkdb.DB(db).Table(table).Run(targetDB)
			if err != nil {
				if strings.Contains(err.Error(), "Table") {
					summary.missingTables++
				} else if strings.Contains(err.Error(), "Database") {
					summary.missingDatabases++
				} else {
					summary.errors++
				}
				continue
			}
			msgs := make(chan map[string]interface{})
			var d map[string]interface{}
			var d2 map[string]interface{}
			cursor.Listen(msgs)
			for msg := range msgs {
				summary.totalDocuments++
				data, _ := json.Marshal(msg)
				json.Unmarshal(data, &d)
				if err := rethinkdb.DB(db).Table(table).Get(d[info["primary_key"].(string)]).ReadOne(&d2, targetDB); err != nil {
					summary.missingDocuments++
					continue
				}
				data2, _ := json.Marshal(d2)
				if len(data) != len(data2) {
					summary.mismatchedDocuments++
				}
			}
		}
	}
	end := time.Since(now)
	ok := summary.totalDocuments - summary.errors - summary.missingDocuments - summary.mismatchedDocuments
	println()
	fmt.Printf("All checked documents (from source server): %v\n", summary.totalDocuments)
	fmt.Printf("Errors: %v\n", summary.errors)
	fmt.Printf("Missing tables: %v\n", summary.missingTables)
	fmt.Printf("Missing databases: %v\n", summary.missingDatabases)
	fmt.Printf("Missing documents: %v\n", summary.missingDocuments)
	fmt.Printf("Documents, which size was not the same: %v\n", summary.mismatchedDocuments)
	fmt.Printf("OK documents: %v (ALL: %v)\n", ok, summary.totalDocuments == ok)
	println()
	fmt.Printf("Time: %v", end.String())
	println()
	return nil
}
