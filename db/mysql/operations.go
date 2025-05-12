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
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v3/db"
)

const (
	laTableSuffix = "_liveattrs_entry"
)

// dropExisting drops existing tables/views.
// It is safe to call this even if one or more of these does not exist.
// Please note that the groupedCorpusName argument represents a derived corpus name
// which is able to group multipe (aligned) corpora together.E.g. 'intercorp_v13_cs'
// and 'intercorp_v13_en' will likely groupedName 'intercorp_v13'. For single corpora,
// the groupedCorpusName is the same as the original one.
func dropExisting(database *sql.DB, groupedCorpusName string) error {
	log.Info().Msg("Attempting to drop possible existing tables and views...")
	var err error
	_, err = database.Exec("DROP TABLE IF EXISTS cache")
	if err != nil {
		return fmt.Errorf("failed to drop table 'cache': %s", err)
	}
	_, err = database.Exec(fmt.Sprintf("DROP VIEW IF EXISTS `%s_bibliography`", groupedCorpusName))
	if err != nil {
		return fmt.Errorf("failed to drop view `%s_bibliography`: %s", groupedCorpusName, err)
	}
	_, err = database.Exec(
		fmt.Sprintf("DROP TABLE IF EXISTS `%s%s`", groupedCorpusName, laTableSuffix))
	if err != nil {
		return fmt.Errorf("failed to drop table '%s%s': %s", groupedCorpusName, laTableSuffix, err)
	}
	_, err = database.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s_colcounts`", groupedCorpusName))
	if err != nil {
		return fmt.Errorf("failed to drop table `%s_colcounts`: %s", groupedCorpusName, err)
	}
	log.Info().Msg("...DONE")
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
	ans[2] = "corpus_id VARCHAR(63)"
	if hasSelfJoin {
		ans[3] = "item_id VARCHAR(127)"

	} else {
		ans = ans[:3]
	}
	return ans
}

func createAuxIndices(database *sql.DB, groupedCorpusName string, cols []string) error {
	var err error
	for _, c := range cols {
		_, err = database.Exec(
			fmt.Sprintf("CREATE INDEX `%s_%s_idx` ON `%s%s`(%s)",
				groupedCorpusName, c, groupedCorpusName, laTableSuffix, c))
		if err != nil {
			return err
		}
		log.Info().
			Str("index", fmt.Sprintf(`%s_%s_idx`, groupedCorpusName, c)).
			Str("table", groupedCorpusName+laTableSuffix).
			Str("column", c).
			Msg("Created custom database index")
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
func createBibView(database *sql.DB, groupedCorpusName string, cols []string, idAttr string) error {
	colDefs := generateViewColDefs(cols, idAttr)
	_, err := database.Exec(fmt.Sprintf(
		"CREATE VIEW %s_bibliography AS SELECT %s FROM `%s%s`",
		groupedCorpusName, joinArgs(colDefs), groupedCorpusName, laTableSuffix))
	if err != nil {
		return err
	}
	return nil
}

// createSchema creates all the required tables, views and indices
func createSchema(
	database *sql.DB,
	groupedCorpusName string,
	structures map[string][]string,
	indexedCols []string,
	useSelfJoin bool,
	countColumns db.VertColumns,
) error {
	log.Info().Msg("Attempting to create tables and views")

	cols := generateColNames(structures)
	colsDefs := make([]string, len(cols))
	for i, col := range cols {
		colsDefs[i] = fmt.Sprintf("%s TEXT", col)
	}
	auxColDefs := generateAuxColDefs(useSelfJoin)
	allCollsDefs := append(colsDefs, auxColDefs...)
	_, dbErr := database.Exec(
		fmt.Sprintf(
			"CREATE TABLE `%s%s` (id INTEGER PRIMARY KEY auto_increment, %s) ENGINE=InnoDB ROW_FORMAT=DYNAMIC",
			groupedCorpusName,
			laTableSuffix,
			joinArgs(allCollsDefs),
		),
	)
	if dbErr != nil {
		return fmt.Errorf(
			"failed to create table '%s%s': %s", groupedCorpusName, laTableSuffix, dbErr)
	}

	if useSelfJoin {
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE UNIQUE INDEX `%s%s_item_id_corpus_id_idx` ON `%s%s`(item_id, corpus_id)",
			groupedCorpusName, laTableSuffix, groupedCorpusName, laTableSuffix))
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index `%s%s_item_id_corpus_id_idx` on `%s%s`(item_id, corpus_id): %s",
				groupedCorpusName, laTableSuffix, groupedCorpusName, laTableSuffix, dbErr)
		}
	}
	dbErr = createAuxIndices(database, groupedCorpusName, indexedCols)
	if dbErr != nil {
		return fmt.Errorf("failed to create a custom index: %s", dbErr)
	}

	if len(countColumns) > 0 {
		colDefs := db.GenerateColCountNames(countColumns)
		for i, c := range colDefs {
			colDefs[i] = c + fmt.Sprintf(" VARCHAR(%d) COLLATE utf8mb4_general_ci", db.DfltColcountVarcharSize)
		}
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE TABLE %s_colcounts ("+
				"%s, hash_id VARCHAR(40), corpus_id VARCHAR(%d), "+
				"count INTEGER, arf FLOAT, initial_cap TINYINT NOT NULL DEFAULT 0, "+
				"ngram_size TINYINT NOT NULL, "+
				"PRIMARY KEY(hash_id)"+
				")",
			groupedCorpusName, strings.Join(colDefs, ", "), db.DfltColcountVarcharSize))
		if dbErr != nil {
			return fmt.Errorf("failed to create table '%s_colcounts': %s", groupedCorpusName, dbErr)
		}
		indexName := fmt.Sprintf("%s_colcounts_corpus_id_idx", groupedCorpusName)
		indexTarget := fmt.Sprintf("%s_colcounts(corpus_id)", groupedCorpusName)
		log.Debug().Str("indexName", indexName).Msg("creating index")
		_, dbErr = database.Exec(fmt.Sprintf("CREATE INDEX %s ON %s", indexName, indexTarget))
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index %s on %s: %s", indexName, indexTarget, dbErr)
		}
		indexName = fmt.Sprintf("%s_colcounts_ngram_size_idx", groupedCorpusName)
		indexTarget = fmt.Sprintf("%s_colcounts(ngram_size)", groupedCorpusName)
		log.Debug().Str("indexName", indexName).Msg("creating index")
		_, dbErr = database.Exec(fmt.Sprintf("CREATE INDEX %s ON %s", indexName, indexTarget))
		if dbErr != nil {
			return fmt.Errorf(
				"failed to create index %s on %s: %s",
				indexName, indexTarget, dbErr)
		}
	}
	log.Info().Msg("Finished creating colcounts table and its indexes")
	return nil
}
