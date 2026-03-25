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
	"time"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v3/cnf"
	"github.com/czcorpus/vert-tagextract/v3/db"

	"github.com/go-sql-driver/mysql"
)

func joinArgs(args []string) string {
	return strings.Join(args, ", ")
}

type Writer struct {
	database *sql.DB
	tx       *sql.Tx
	dbName   string

	// groupedCorpusName represents a derived corpus name which is able to group multiple
	// (aligned) corpora together (e.g. intercorp_v13_en, intercorp_v13_cs => intercorp_v13)
	groupedCorpusName string

	Structures   map[string][]string
	IndexedCols  []string
	SelfJoinConf db.SelfJoinConf
	BibViewConf  db.BibViewConf
	CountColumns db.VertColumns
}

func (w *Writer) DatabaseExists() bool {
	row := w.database.QueryRow(
		`SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		w.dbName, w.groupedCorpusName+"_liveattrs_entry",
	)
	var ans bool
	err := row.Scan(&ans)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		log.Error().Err(err).Msg("failed to test data storage existence")
		return false
	}
	return ans
}

func (w *Writer) Initialize(appendMode bool) error {
	tmpExist, err := testTmpTablesExist(w.database, w.dbName, w.groupedCorpusName)
	if err != nil {
		return fmt.Errorf("failed to initialize process: %w", err)
	}
	if tmpExist {
		return fmt.Errorf("cannot initialize process - _new tables found - possible conflict")
	}

	if err := createSchema(
		w.database,
		w.groupedCorpusName,
		w.Structures,
		w.SelfJoinConf.IsConfigured(),
		w.CountColumns,
	); err != nil {
		return fmt.Errorf("cannot initialize process: %w", err)
	}

	w.tx, err = w.database.Begin()
	if err != nil {
		return fmt.Errorf("cannot initialize process: %w", err)
	}

	if appendMode {
		if err := copyPrevDataToTmp(w.tx, w.groupedCorpusName); err != nil {
			return fmt.Errorf("cannot initialize process: %w", err)
		}
	}
	return nil
}

func (w *Writer) tableExists(tableName string) (bool, error) {
	row := w.database.QueryRow(
		"SELECT COUNT(*) FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = ? "+
			"AND TABLE_NAME = ? ",
		w.dbName, tableName,
	)
	var ans int
	if err := row.Scan(&ans); err != nil {
		return false, fmt.Errorf("failed to test existence of the %s table: %w", tableName, err)
	}
	return ans == 1, nil
}

// Finalize renames working tables to their production name effectively
// replacing the current ones. It also creates necessary indexes for
// smoother search.
func (w *Writer) Finalize() error {
	if err := createIndexes(
		w.database,
		w.groupedCorpusName,
		w.IndexedCols,
		w.SelfJoinConf.IsConfigured(),
		len(w.CountColumns) > 0,
	); err != nil {
		return err
	}

	if len(w.CountColumns) > 0 {
		colcExists, err := w.tableExists(fmt.Sprintf("%s_colcounts", w.groupedCorpusName))
		if err != nil {
			return err
		}
		log.Info().
			Str("targetTable", w.groupedCorpusName+"_colcounts").
			Msg("renaming temporary _new table to the production form")
		if colcExists {
			if _, err := w.database.Exec(
				fmt.Sprintf(
					"RENAME TABLE %s_colcounts TO %s_colcounts_old, %s_colcounts_new TO  %s_colcounts",
					w.groupedCorpusName, w.groupedCorpusName, w.groupedCorpusName, w.groupedCorpusName,
				),
			); err != nil {
				return fmt.Errorf("failed to rename colcounts table to the production form: %w", err)
			}

		} else {
			if _, err := w.database.Exec(
				fmt.Sprintf(
					"RENAME TABLE %s_colcounts_new TO  %s_colcounts",
					w.groupedCorpusName, w.groupedCorpusName,
				),
			); err != nil {
				return fmt.Errorf("failed to rename colcounts table to the production form: %w", err)
			}
		}
		if _, err := w.database.Exec(
			fmt.Sprintf("DROP TABLE IF EXISTS %s_colcounts_old", w.groupedCorpusName),
		); err != nil {
			return fmt.Errorf("failed to drop table %s_colcounts_old: %w", w.groupedCorpusName, err)
		}
	}

	laExists, err := w.tableExists(fmt.Sprintf("%s_liveattrs_entry", w.groupedCorpusName))
	if err != nil {
		return err
	}
	log.Info().
		Str("targetTable", w.groupedCorpusName+"_liveattrs_entry").
		Msg("renaming temporary _new table to the production form")
	if laExists {
		if _, err := w.database.Exec(
			fmt.Sprintf(
				"RENAME TABLE %s_liveattrs_entry TO %s_liveattrs_entry_old, %s_liveattrs_entry_new TO %s_liveattrs_entry",
				w.groupedCorpusName, w.groupedCorpusName, w.groupedCorpusName, w.groupedCorpusName,
			),
		); err != nil {
			return fmt.Errorf("failed to rename colcounts table to the production form: %w", err)
		}

	} else {
		if _, err := w.database.Exec(
			fmt.Sprintf(
				"RENAME TABLE %s_liveattrs_entry_new TO %s_liveattrs_entry",
				w.groupedCorpusName, w.groupedCorpusName,
			),
		); err != nil {
			return fmt.Errorf("failed to rename colcounts table to the production form: %w", err)
		}
	}
	if _, err := w.database.Exec(
		fmt.Sprintf("DROP TABLE IF EXISTS %s_liveattrs_entry_old", w.groupedCorpusName),
	); err != nil {
		return fmt.Errorf("failed to drop table %s_liveattrs_entry_old: %w", w.groupedCorpusName, err)
	}

	bibExists, err := testBibViewExists(w.database, w.dbName, w.groupedCorpusName)
	if err != nil {
		return fmt.Errorf("failed to test bib. view existence: %w", err)
	}
	if !bibExists && w.BibViewConf.IsConfigured() {
		if err := createBibView(
			w.database, w.groupedCorpusName, w.BibViewConf.Cols, w.BibViewConf.IDAttr,
		); err != nil {
			return fmt.Errorf("failed to create bibliography view on _liveattrs_entry: %w", err)
		}
	}
	return nil
}

func (w *Writer) PrepareInsert(tableNameSuff string, attrs []string) (db.InsertOperation, error) {
	if w.tx == nil {
		return nil, fmt.Errorf("cannot prepare insert into %s - no transaction active", tableNameSuff)
	}
	valReplac := make([]string, len(attrs))
	for i := range attrs {
		valReplac[i] = "?"
	}
	stmt, err := w.tx.Prepare(
		fmt.Sprintf(
			"INSERT INTO `%s_%s` (%s) VALUES (%s)",
			w.groupedCorpusName,
			tableNameSuff,
			joinArgs(attrs),
			joinArgs(valReplac),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare INSERT into %s: %s", tableNameSuff, err)
	}
	return &db.Insert{Stmt: stmt}, nil
}

func (w *Writer) RemoveRecordsOlderThan(date string, attr db.DateAttr) (int, error) {
	res, err := w.tx.Exec(
		fmt.Sprintf(
			"DELETE FROM %s%s WHERE STR_TO_DATE(%s, '%%Y-%%m-%%d') < ?",
			w.groupedCorpusName, laTableSuffix, attr.String()),
		date,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to move data window: %w", err)
	}
	numRows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to determine number of removed rows: %w", err)
	}
	return int(numRows), nil
}

func (w *Writer) Commit() error {
	return w.tx.Commit()
}

func (w *Writer) Rollback() error {
	return w.tx.Rollback()
}

func (w *Writer) Close() {
	err := w.database.Close()
	if err != nil {
		log.Warn().Err(err).Msg("error closing database")
	}
}

func NewWriter(conf *cnf.VTEConf) (*Writer, error) {

	mconf := mysql.NewConfig()
	mconf.Net = "tcp"
	mconf.Addr = conf.DB.Host
	mconf.User = conf.DB.User
	mconf.Passwd = conf.DB.Password
	mconf.DBName = conf.DB.Name
	mconf.ParseTime = true
	mconf.Loc = time.Local
	db, err := sql.Open("mysql", mconf.FormatDSN())
	if err != nil {
		return nil, err
	}
	groupedCorpusName := conf.Corpus
	if conf.ParallelCorpus != "" {
		groupedCorpusName = conf.ParallelCorpus
	}
	return &Writer{
		database:          db,
		dbName:            conf.DB.Name,
		groupedCorpusName: groupedCorpusName,
		Structures:        conf.Structures,
		IndexedCols:       conf.IndexedCols,
		SelfJoinConf:      conf.SelfJoin,
		BibViewConf:       conf.BibView,
		CountColumns:      conf.Ngrams.VertColumns,
	}, nil
}
