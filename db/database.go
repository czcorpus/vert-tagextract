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

/*
This file contains all the database operations
required to create a proper schema for
liveattrs (tables and their indices, views)
*/

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/czcorpus/vert-tagextract/vteconf"
	_ "github.com/mattn/go-sqlite3" // load the driver
)

// OpenDatabase opens a sqlite3 database specified by
// its filesystem path. In case of an error it panics.
func OpenDatabase(dbPath string) *sql.DB {
	var err error
	if db, err := sql.Open("sqlite3", dbPath); err == nil {
		return db
	}
	panic(err)
}

// generateColNames produces a list of structural
// attribute names as used in database
// (i.e. [structname]_[attr_name]) out of lists
// of structural attributes defined in the configuration.
// (see _examples/*.json)
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

// generateAuxColDefs creates definitions for
// auxiliary columns (num of positions, num of words etc.)
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

// generateViewColDefs creates definitions for
// bibliography view
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

// CreateBibView creates a database view needed
// by liveattrs to fetch bibliography information.
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
		log.Printf("Created custom index %s_idx on item(%s)", c, c)
	}
	return nil
}

// DropExisting drops existing tables/views.
// It is safe to call this even if one or more
// of these does not exist.
func DropExisting(database *sql.DB) {
	log.Print("Attempting to drop possible existing tables and views...")
	var err error
	_, err = database.Exec("DROP TABLE IF EXISTS cache")
	if err != nil {
		log.Fatalf("Failed to drop table 'cache': %s", err)
	}
	_, err = database.Exec("DROP VIEW IF EXISTS bibliography")
	if err != nil {
		log.Fatalf("Failed to drop view 'bibliography': %s", err)
	}
	_, err = database.Exec("DROP TABLE IF EXISTS item")
	if err != nil {
		log.Fatalf("Failed to drop table 'item': %s", err)
	}
	log.Print("...DONE")
}

// CreateSchema creates all the required tables, views and indices
func CreateSchema(database *sql.DB, conf *vteconf.VTEConf) {
	log.Print("Attempting to create tables and views...")

	var dbErr error
	_, dbErr = database.Exec("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT)")
	if dbErr != nil {
		log.Fatalf("Failed to create table 'cache': %s", dbErr)
	}

	cols := generateColNames(conf)
	colsDefs := make([]string, len(cols))
	for i, col := range cols {
		colsDefs[i] = fmt.Sprintf("%s TEXT", col)
	}
	auxColDefs := generateAuxColDefs(conf.UsesSelfJoin())
	allCollsDefs := append(colsDefs, auxColDefs...)
	_, dbErr = database.Exec(fmt.Sprintf("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)", joinArgs(allCollsDefs)))
	if dbErr != nil {
		log.Fatalf("Failed to create table 'item': %s", dbErr)
	}

	if conf.UsesSelfJoin() {
		_, dbErr = database.Exec("CREATE UNIQUE INDEX item_id_corpus_id_idx ON item(item_id, corpus_id)")
		if dbErr != nil {
			log.Fatalf("Failed to create index item_id_idx on item(item_id): %s", dbErr)
		}
	}
	dbErr = createAuxIndices(database, conf.IndexedCols)
	if dbErr != nil {
		log.Fatalf("Failed to create a custom index: %s", dbErr)
	}

	log.Print("...DONE")
}
