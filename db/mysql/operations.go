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
	laTableSuffix    = "_liveattrs_entry"
	laTableSuffixTMP = "_liveattrs_entry_new"
)

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

func testBibViewExists(
	database *sql.DB,
	dbName string,
	groupedCorpusName string,
) (bool, error) {
	row := database.QueryRow(
		"SELECT COUNT(*) "+
			"FROM information_schema.views "+
			"WHERE table_schema = ? "+
			"AND table_name = ? ",
		dbName,
		groupedCorpusName+"_bibliography",
	)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		return false, fmt.Errorf("failed to determine bib view existence: %w", err)
	}
	return cnt == 1, nil
}

// createSchema creates all the required tables, views and indices. It defines tables
// with name containing the _new suffix so a possible current production table is still
// operational. Once everything is done, the tables are expected to be renamed to their
// final production name replacing the old ones.
func createSchema(
	database *sql.DB,
	groupedCorpusName string,
	structures map[string][]string,
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
			laTableSuffixTMP,
			joinArgs(allCollsDefs),
		),
	)
	if dbErr != nil {
		return fmt.Errorf(
			"failed to create table '%s%s': %s", groupedCorpusName, laTableSuffixTMP, dbErr)
	}

	if len(countColumns) > 0 {
		colDefs := db.GenerateColCountNames(countColumns)
		for i, c := range colDefs {
			colDefs[i] = c + fmt.Sprintf(" VARCHAR(%d) COLLATE utf8mb4_general_ci", db.DfltColcountVarcharSize)
		}
		_, dbErr = database.Exec(fmt.Sprintf(
			"CREATE TABLE %s_colcounts_new ("+
				"%s, hash_id VARCHAR(40), corpus_id VARCHAR(%d), "+
				"count INTEGER, arf FLOAT, initial_cap TINYINT NOT NULL DEFAULT 0, "+
				"ngram_size TINYINT NOT NULL, "+
				"PRIMARY KEY(hash_id)"+
				")",
			groupedCorpusName, strings.Join(colDefs, ", "), db.DfltColcountVarcharSize))
		if dbErr != nil {
			return fmt.Errorf("failed to create table '%s_colcounts': %s", groupedCorpusName, dbErr)
		}
	}
	log.Info().Msg("Finished creating colcounts table and its indexes")
	return nil
}

// removeDuplicateItemCorpusRows checks whether the working table contains
// multiple rows sharing the same (item_id, corpus_id) pair (which would make
// creating a unique index on these columns fail) and, if so, removes the
// redundant rows, keeping only the one with the lowest id for each pair.
func removeDuplicateItemCorpusRows(database *sql.DB, groupedCorpusName string) error {
	table := fmt.Sprintf("%s%s", groupedCorpusName, laTableSuffixTMP)

	var numDuplicateKeys int
	row := database.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM (SELECT item_id, corpus_id FROM `%s` "+
			"GROUP BY item_id, corpus_id HAVING COUNT(*) > 1) AS dups",
		table,
	))
	if err := row.Scan(&numDuplicateKeys); err != nil {
		return fmt.Errorf("failed to check for duplicate item_id/corpus_id entries in `%s`: %w", table, err)
	}
	if numDuplicateKeys == 0 {
		return nil
	}

	log.Warn().
		Int("numDuplicateKeys", numDuplicateKeys).
		Str("table", table).
		Msg("found duplicate (item_id, corpus_id) entries, removing redundant rows")

	if _, err := database.Exec(fmt.Sprintf(
		"DELETE t1 FROM `%s` AS t1 INNER JOIN `%s` AS t2 "+
			"ON t1.item_id = t2.item_id AND t1.corpus_id = t2.corpus_id AND t1.id > t2.id",
		table, table,
	)); err != nil {
		return fmt.Errorf("failed to remove duplicate item_id/corpus_id rows from `%s`: %w", table, err)
	}
	return nil
}

// createIndexes generates indexes on the final tables
func createIndexes(
	database *sql.DB,
	groupedCorpusName string,
	indexedCols []string,
	useSelfJoin bool,
	useCountColumns bool,
) error {
	if useSelfJoin {
		if err := removeDuplicateItemCorpusRows(database, groupedCorpusName); err != nil {
			return err
		}
		if _, err := database.Exec(fmt.Sprintf(
			"CREATE UNIQUE INDEX `%s%s_item_id_corpus_id_idx` ON `%s%s`(item_id, corpus_id)",
			groupedCorpusName, laTableSuffix, groupedCorpusName, laTableSuffixTMP)); err != nil {
			return fmt.Errorf(
				"failed to create index `%s%s_item_id_corpus_id_idx` on `%s%s`(item_id, corpus_id): %s",
				groupedCorpusName, laTableSuffix, groupedCorpusName, laTableSuffixTMP, err)
		}
	}

	if useCountColumns {
		indexName := fmt.Sprintf("%s_colcounts_corpus_id_idx", groupedCorpusName)
		indexTarget := fmt.Sprintf("%s_colcounts_new(corpus_id)", groupedCorpusName)
		log.Debug().Str("indexName", indexName).Msg("creating index")
		if _, err := database.Exec(fmt.Sprintf("CREATE INDEX %s ON %s", indexName, indexTarget)); err != nil {
			return fmt.Errorf(
				"failed to create index %s on %s: %s", indexName, indexTarget, err)
		}
		indexName = fmt.Sprintf("%s_colcounts_ngram_size_idx", groupedCorpusName)
		indexTarget = fmt.Sprintf("%s_colcounts_new(ngram_size)", groupedCorpusName)
		log.Debug().Str("indexName", indexName).Msg("creating index")
		if _, err := database.Exec(fmt.Sprintf("CREATE INDEX %s ON %s", indexName, indexTarget)); err != nil {
			return fmt.Errorf(
				"failed to create index %s on %s: %s",
				indexName, indexTarget, err)
		}
	}
	// auxiliary indexes
	for _, c := range indexedCols {
		_, err := database.Exec(
			fmt.Sprintf("CREATE INDEX `%s_%s_idx` ON `%s%s`(%s)",
				groupedCorpusName, c, groupedCorpusName, laTableSuffixTMP, c))
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

func copyPrevDataToTmp(tx *sql.Tx, groupedCorpusName string) error {
	if _, err := tx.Exec(
		fmt.Sprintf(
			"INSERT INTO %s_liveattrs_entry_new SELECT * FROM %s_liveattrs_entry",
			groupedCorpusName,
			groupedCorpusName,
		),
	); err != nil {
		return fmt.Errorf("failed to copy previous data to the working _new table (append mode): %w", err)
	}
	return nil
}

func testTmpTablesExist(
	database *sql.DB,
	dbName string,
	groupedCorpusName string,
) (bool, error) {
	row := database.QueryRow(
		fmt.Sprintf(
			"SELECT TABLE_NAME FROM information_schema.TABLES "+
				"WHERE TABLE_SCHEMA = ? "+
				" AND TABLE_NAME IN ('%s_colcounts_new', '%s_liveattrs_entry_new') "+
				" LIMIT 1",
			groupedCorpusName,
			groupedCorpusName,
		),
		dbName,
	)
	var tn string
	if err := row.Scan(&tn); err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("failed to check for working _new tables: %w", err)
	}
	return tn != "", nil
}
