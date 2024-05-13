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

import (
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v2/db"
	"github.com/czcorpus/vert-tagextract/v2/fs"
)

// -------------------------------

type Writer struct {
	database       *sql.DB
	tx             *sql.Tx
	Path           string
	PreconfQueries []string
	Structures     map[string][]string
	IndexedCols    []string
	SelfJoinConf   db.SelfJoinConf
	BibViewConf    db.BibViewConf
	CountColumns   []int
}

func (w *Writer) DatabaseExists() bool {
	return fs.IsFile(w.Path)
}

func (w *Writer) Initialize(appendMode bool) error {
	var err error
	dbExisted := fs.IsFile(w.Path)
	w.database, err = openDatabase(w.Path)
	if err != nil {
		return err
	}
	log.Info().Msgf("Opened sqlite3 database %s", w.Path)

	if !appendMode {
		if dbExisted {
			log.
				Warn().
				Str("database", w.Path).
				Msg("The database already exists. Existing data will be deleted.")
			err := dropExisting(w.database)
			if err != nil {
				return err
			}
		}
		err := createSchema(
			w.database,
			w.Structures,
			w.IndexedCols,
			w.SelfJoinConf.IsConfigured(),
			w.CountColumns,
		)
		if err != nil {
			return err
		}
		if w.BibViewConf.IsConfigured() {
			err := createBibView(w.database, w.BibViewConf.Cols, w.BibViewConf.IDAttr)
			if err != nil {
				return err
			}
		}
	}

	var dbConf []string
	if len(w.PreconfQueries) > 0 {
		dbConf = w.PreconfQueries

	} else {
		log.Warn().Msg("No pre-configuration queries found, using default")
		dbConf = []string{
			"PRAGMA synchronous = OFF",
			"PRAGMA journal_mode = MEMORY",
		}
	}
	for _, cnf := range dbConf {
		log.Info().Str("value", cnf).Msg("Applying preconfiguration")
		w.database.Exec(cnf)
	}
	w.tx, err = w.database.Begin()
	return err
}

func (w *Writer) CreateBibView(cols []string, idAttr string) error {
	return createBibView(w.database, cols, idAttr)
}

func (w *Writer) PrepareInsert(table string, attrs []string) (db.InsertOperation, error) {
	if w.tx == nil {
		return nil, fmt.Errorf("cannot prepare insert - no transaction active")
	}
	stmt, err := prepareInsert(w.tx, table, attrs)
	if err != nil {
		return nil, err
	}
	return &db.Insert{Stmt: stmt}, nil
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
		log.Warn().Err(err).Msg("Error closing database")
	}
}
