// Copyright 2022 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2022 Charles University, Faculty of Arts,
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

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/czcorpus/vert-tagextract/v2/db"
)

// dropExisting drops existing tables/views.
// It is safe to call this even if one or more
// of these does not exist.
func dropExisting(database *sql.DB, corpusName string) error {
	log.Print("Attempting to drop possible existing tables and views...")
	var err error
	_, err = database.Exec("DROP TABLE IF EXISTS cache")
	if err != nil {
		return fmt.Errorf("failed to drop table 'cache': %s", err)
	}
	_, err = database.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s_bibliography", corpusName))
	if err != nil {
		return fmt.Errorf("failed to drop view '%s_bibliography': %s", corpusName, err)
	}
	_, err = database.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_item", corpusName))
	if err != nil {
		return fmt.Errorf("failed to drop table '%s_item': %s", corpusName, err)
	}
	_, err = database.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_colcounts", corpusName))
	if err != nil {
		return fmt.Errorf("failed to drop table '%s_colcounts': %s", corpusName, err)
	}
	log.Print("...DONE")
	return nil
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

// generateAuxColDefs creates definitions for
// auxiliary columns (num of positions, num of words etc.)
func generateAuxColDefs(hasSelfJoin bool) []string {
	ans := make([]string, 4)
	ans[0] = "poscount INTEGER"
	ans[1] = "wordcount INTEGER"
	ans[2] = "corpus_id VARCHAR(127)"
	if hasSelfJoin {
		ans[3] = "item_id VARCHAR(255)"

	} else {
		ans = ans[:3]
	}
	return ans
}

func createAuxIndices(database *sql.DB, corpusName string, cols []string) error {
	var err error
	for _, c := range cols {
		_, err = database.Exec(
			fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s_item(%s)",
				corpusName, c, corpusName, c))
		if err != nil {
			return err
		}
		log.Printf("Created custom index %s_%s_idx on %s_item(%s)",
			corpusName, c, corpusName, c)
	}
	return nil
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
func createBibView(database *sql.DB, corpusName string, cols []string, idAttr string) error {
	colDefs := generateViewColDefs(cols, idAttr)
	_, err := database.Exec(fmt.Sprintf(
		"CREATE VIEW %s_bibliography AS SELECT %s FROM %s_item",
		corpusName, joinArgs(colDefs), corpusName))
	if err != nil {
		return err
	}
	return nil
}

// createSchema creates all the required tables, views and indices
func createSchema(
	database *sql.DB,
	corpusName string,
	structures map[string][]string,
	indexedCols []string,
	useSelfJoin bool,
	countColumns []int,
) error {
	log.Print("Attempting to create tables and views...")

	cols := generateColNames(structures)
	colsDefs := make([]string, len(cols))
	for i, col := range cols {
		colsDefs[i] = fmt.Sprintf("%s VARCHAR(255)", col)
	}
	auxColDefs := generateAuxColDefs(useSelfJoin)
	allCollsDefs := append(colsDefs, auxColDefs...)
	_, dbErr := database.Exec(
		fmt.Sprintf("CREATE TABLE %s_item (id INTEGER PRIMARY KEY auto_increment, %s)", corpusName, joinArgs(allCollsDefs)))
	if dbErr != nil {
		return fmt.Errorf("failed to create table '%s_item': %s", corpusName, dbErr)
	}

	if useSelfJoin {
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE UNIQUE INDEX %s_item_item_id_corpus_id_idx ON %s_item(item_id, corpus_id)",
			corpusName, corpusName))
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index %s_item_item_id_corpus_id_idx on %s_item(item_id, corpus_id): %s",
				corpusName, corpusName, dbErr)
		}
	}
	dbErr = createAuxIndices(database, corpusName, indexedCols)
	if dbErr != nil {
		return fmt.Errorf("failed to create a custom index: %s", dbErr)
	}

	if len(countColumns) > 0 {
		columns := db.GenerateColCountNames(countColumns)
		colDefs := db.GenerateColCountNames(countColumns)
		for i, c := range colDefs {
			colDefs[i] = c + " VARCHAR(127)"
		}
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE TABLE %s_colcounts (%s, corpus_id VARCHAR(127), count INTEGER, arf INTEGER, PRIMARY KEY(%s))",
			corpusName, strings.Join(colDefs, ", "), strings.Join(columns, ", ")))
		if dbErr != nil {
			return fmt.Errorf("failed to create table '%s_colcounts': %s", corpusName, dbErr)
		}
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE INDEX %s_colcounts_corpus_id_idx ON %s_colcounts(corpus_id)",
			corpusName, corpusName))
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index colcounts_corpus_id_idx on %s_colcounts(corpus_id): %s",
				corpusName, dbErr)
		}
	}
	log.Print("...DONE")
	return nil
}
