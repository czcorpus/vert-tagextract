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

package sqlite

/*
This file contains all the database operations
required to create a proper schema for
liveattrs (tables and their indices, views)
*/

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v2/db"

	_ "github.com/mattn/go-sqlite3" // load the driver
)

// openDatabase opens a sqlite3 database specified by
// its filesystem path. In case of an error it panics.
func openDatabase(dbPath string) (*sql.DB, error) {
	var err error
	if db, err := sql.Open("sqlite3", dbPath); err == nil {
		return db, nil
	}
	return nil, fmt.Errorf("failed to open text types db: %s", err)
}

// prepareInsert creates a prepared statement for an INSERT
// operation.
func prepareInsert(database *sql.Tx, table string, cols []string) (*sql.Stmt, error) {
	valReplac := make([]string, len(cols))
	for i := range cols {
		valReplac[i] = "?"
	}
	ans, err := database.Prepare(
		fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, joinArgs(cols), joinArgs(valReplac)))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare INSERT: %s", err)
	}
	return ans, nil
}

// generateColNames produces a list of structural
// attribute names as used in database
// (i.e. [structname]_[attr_name]) out of lists
// of structural attributes defined in the configuration.
// (see _examples/*.json)
func generateColNames(structures map[string][]string) []string {
	numAttrs := 0
	for _, v := range structures {
		numAttrs += len(v)
	}
	ans := make([]string, numAttrs)
	i := 0
	for k, v := range structures {
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
func generateViewColDefs(cols []string, idAttr string) []string {
	ans := make([]string, len(cols))
	for i, c := range cols {
		if c != idAttr {
			ans[i] = c

		} else {
			ans[i] = fmt.Sprintf("%s AS id", c)
		}
	}
	return ans
}

// createBibView creates a database view needed
// by liveattrs to fetch bibliography information.
func createBibView(database *sql.DB, cols []string, idAttr string) error {
	colDefs := generateViewColDefs(cols, idAttr)
	_, err := database.Exec(fmt.Sprintf("CREATE VIEW bibliography AS SELECT %s FROM liveattrs_entry", joinArgs(colDefs)))
	if err != nil {
		return err
	}
	return nil
}

func createAuxIndices(database *sql.DB, cols []string) error {
	var err error
	for _, c := range cols {
		_, err = database.Exec(fmt.Sprintf("CREATE INDEX %s_idx ON liveattrs_entry(%s)", c, c))
		if err != nil {
			return err
		}
		log.Info().
			Str("index", c+"_idx").
			Str("table", "liveattrs_entry").
			Str("column", c).
			Msg("Created custom index")
	}
	return nil
}

// dropExisting drops existing tables/views.
// It is safe to call this even if one or more
// of these does not exist.
func dropExisting(database *sql.DB) error {
	log.Info().Msg("Attempting to drop possible existing tables and views")
	var err error
	_, err = database.Exec("DROP TABLE IF EXISTS cache")
	if err != nil {
		return fmt.Errorf("failed to drop table 'cache': %s", err)
	}
	_, err = database.Exec("DROP VIEW IF EXISTS bibliography")
	if err != nil {
		return fmt.Errorf("failed to drop view 'bibliography': %s", err)
	}
	_, err = database.Exec("DROP TABLE IF EXISTS liveattrs_entry")
	if err != nil {
		return fmt.Errorf("failed to drop table 'liveattrs_entry': %s", err)
	}
	_, err = database.Exec("DROP TABLE IF EXISTS colcounts")
	if err != nil {
		return fmt.Errorf("failed to drop table 'colcounts': %s", err)
	}
	return nil
}

// createSchema creates all the required tables, views and indices
func createSchema(
	database *sql.DB,
	structures map[string][]string,
	indexedCols []string,
	useSelfJoin bool,
	countColumns db.VertColumns,
) error {
	log.Info().Msg("Attempting to create tables and views")

	var dbErr error
	_, dbErr = database.Exec("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT)")
	if dbErr != nil {
		return fmt.Errorf("failed to create table 'cache': %s", dbErr)
	}

	cols := generateColNames(structures)
	colsDefs := make([]string, len(cols))
	for i, col := range cols {
		colsDefs[i] = fmt.Sprintf("%s TEXT", col)
	}
	auxColDefs := generateAuxColDefs(useSelfJoin)
	allCollsDefs := append(colsDefs, auxColDefs...)
	_, dbErr = database.Exec(fmt.Sprintf("CREATE TABLE liveattrs_entry (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)", joinArgs(allCollsDefs)))
	if dbErr != nil {
		return fmt.Errorf("failed to create table 'liveattrs_entry': %s", dbErr)
	}

	if useSelfJoin {
		_, dbErr = database.Exec(
			"CREATE UNIQUE INDEX item_id_corpus_id_idx ON liveattrs_entry(item_id, corpus_id)")
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index item_id_idx on liveattrs_entry(item_id): %s", dbErr)
		}
	}
	dbErr = createAuxIndices(database, indexedCols)
	if dbErr != nil {
		return fmt.Errorf("failed to create a custom index: %s", dbErr)
	}

	if len(countColumns) > 0 {
		columns := db.GenerateColCountNames(countColumns)
		colDefs := db.GenerateColCountNames(countColumns)
		for i, c := range colDefs {
			colDefs[i] = c + " TEXT"
		}
		_, dbErr = database.Exec(fmt.Sprintf("CREATE TABLE colcounts (%s, corpus_id TEXT, count INTEGER, arf INTEGER, PRIMARY KEY(%s))",
			strings.Join(colDefs, ", "), strings.Join(columns, ", ")))
		if dbErr != nil {
			return fmt.Errorf("failed to create table 'colcounts': %s", dbErr)
		}
		_, dbErr = database.Exec("CREATE INDEX colcounts_corpus_id_idx ON colcounts(corpus_id)")
		if dbErr != nil {
			return fmt.Errorf("failed to create index colcounts_corpus_id_idx on colcounts(corpus_id): %s", dbErr)
		}
	}
	return nil
}
