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

package factory

import (
	"fmt"
	"vert-tagextract/v2/cnf"
	"vert-tagextract/v2/db"
	"vert-tagextract/v2/db/sqlite"
)

type NullWriter struct {
}

func (nw *NullWriter) DatabaseExists() bool {
	return false
}

func (nw *NullWriter) Initialize(appendMode bool) error {
	return fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) CreateSchema(
	structures map[string][]string,
	indexedCols []string,
	useSelfJoin bool,
	countColumns []int,
) error {
	return fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) CreateBibView(cols []string, idAttr string) error {
	return fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) PrepareInsert(table string, attrs []string) (db.InsertOperation, error) {
	return nil, fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) Commit() error {
	return fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) Rollback() error {
	return fmt.Errorf("no valid database writer installed")
}

func (nw *NullWriter) Close() {}

func NewDatabaseWriter(conf *cnf.VTEConf) db.Writer {
	switch conf.DB.DBType {
	case "sqlite":
		db := &sqlite.Writer{
			Structures:   conf.Structures,
			IndexedCols:  conf.IndexedCols,
			SelfJoinConf: conf.SelfJoin,
			BibViewConf:  conf.BibView,
			CountColumns: conf.Ngrams.AttrColumns,
		}
		return db
	default:
		return &NullWriter{}
	}
}
