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
	"fmt"
)

// SelfJoinConf contains information about aligned
// structural attributes (e.g. sentences from two
// languages).
type SelfJoinConf struct {
	ArgColumns  []string `json:"argColumns"`
	GeneratorFn string   `json:"generatorFn"`
}

func (c *SelfJoinConf) IsConfigured() bool {
	return c.GeneratorFn != ""
}

// ---

// BibViewConf is a sub-configuration for
// bibliographic data.
type BibViewConf struct {
	Cols   []string `json:"cols"`
	IDAttr string   `json:"idAttr"`
}

func (c *BibViewConf) IsConfigured() bool {
	return c.IDAttr != "" && len(c.Cols) > 0
}

type Conf struct {
	Type           string   `json:"type"`
	Name           string   `json:"name"`
	User           string   `json:"user"`
	Password       string   `json:"password"`
	PreconfQueries []string `json:"preconfSettings"`
}

type Writer interface {
	DatabaseExists() bool
	Initialize(appendMode bool) error
	CreateSchema(
		structures map[string][]string,
		indexedCols []string,
		useSelfJoin bool,
		countColumns []int,
	) error
	CreateBibView(cols []string, idAttr string) error
	PrepareInsert(table string, attrs []string) (InsertOperation, error)
	Commit() error
	Rollback() error
	Close()
}

type InsertOperation interface {
	Exec(values ...any) error
}

// GenerateColCountNames creates a list of general column names
// for positional attributes we would like to count. E.g. in
// case we want [0, 1, 3] (this can be something like 'word', 'lemma' )
func GenerateColCountNames(colCount []int) []string {
	columns := make([]string, len(colCount))
	for i, v := range colCount {
		columns[i] = fmt.Sprintf("col%d", v)
	}
	return columns
}
