// Copyright 2017 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2017 Charles University, Faculty of Arts,
//                Institute of the Czech National Corpus
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/czcorpus/vert-tagextract/vteconf"
	_ "github.com/mattn/go-sqlite3" // load the driver
)

func OpenDatabase(dbPath string) *sql.DB {
	var err error
	if db, err := sql.Open("sqlite3", dbPath); err == nil {
		return db
	}
	panic(err)
}

func generateColNames(conf *vteconf.VTEConf) []string {
	numAttrs := 1 // 1st = corpus_id
	for _, v := range conf.Structures {
		numAttrs += len(v)
	}
	ans := make([]string, numAttrs)
	i := 0
	for k, v := range conf.Structures {
		for _, a := range v {
			ans[i] = fmt.Sprintf("%s_%s", k, a)
			i++
		}
	}
	return ans
}

func joinArgs(args []string) string {
	return strings.Join(args, ", ")
}

func generateAuxColDefs(hasSelfJoin bool) []string {
	ans := make([]string, 4)
	ans[0] = "poscount INTEGER"
	ans[1] = "wordcount INTEGER"
	ans[2] = "corpus_id TEXT"
	if hasSelfJoin {
		ans[3] = "item_id STRING"

	} else {
		ans = ans[:3]
	}
	return ans
}

func generateViewColDefs(conf *vteconf.BibViewConf) []string {
	ans := make([]string, len(conf.Cols))
	for i, c := range conf.Cols {
		if c != conf.IDAttr {
			ans[i] = c

		} else {
			ans[i] = fmt.Sprintf("%s as id", c)
		}
	}
	return ans
}

func CreateBibView(database *sql.DB, conf *vteconf.VTEConf) {
	colDefs := generateViewColDefs(&conf.BibView)
	_, err := database.Exec(fmt.Sprintf("CREATE VIEW bibliography AS SELECT %s FROM item", joinArgs(colDefs)))
	if err != nil {
		panic(err)
	}
}

func createAuxIndices(database *sql.DB, cols []string) error {
	var err error
	for _, c := range cols {
		_, err = database.Exec(fmt.Sprintf("CREATE INDEX %s_idx ON item(%s)", c, c))
		if err != nil {
			return err
		}
	}
	return nil
}

func DropExisting(database *sql.DB) {
	database.Exec("DROP TABLE IF EXISTS cache")
	database.Exec("DROP VIEW IF EXISTS bibliography")
	database.Exec("DROP TABLE IF EXISTS item")
}

func CreateSchema(database *sql.DB, conf *vteconf.VTEConf) {
	var dbErr error

	log.Print("Attempting to create table 'cache'...")
	_, dbErr = database.Exec("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT)")
	if dbErr != nil {
		log.Fatal(dbErr)
	}
	log.Print("...DONE")

	log.Print("Attempting to create table 'item'...")
	cols := generateColNames(conf)
	colsDefs := make([]string, len(cols))
	for i, col := range cols {
		colsDefs[i] = fmt.Sprintf("%s TEXT", col)
	}
	auxColDefs := generateAuxColDefs(conf.UsesSelfJoin())
	allCollsDefs := append(colsDefs, auxColDefs...)
	_, dbErr = database.Exec(fmt.Sprintf("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)", joinArgs(allCollsDefs)))
	if dbErr != nil {
		log.Fatal(dbErr)
	}
	log.Print("...DONE")
	log.Printf("Attempting to create indices...")
	if conf.UsesSelfJoin() {
		_, dbErr = database.Exec("CREATE INDEX item_id_idx ON item(item_id)")
		if dbErr != nil {
			log.Fatal(dbErr)
		}
	}
	dbErr = createAuxIndices(database, conf.IndexedCols)
	if dbErr != nil {
		log.Fatal(dbErr)
	}
	log.Print("...DONE")
}
