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
	"strings"
)

const (
	// DfltColcountVarcharSize specifies a max. size
	// for VARCHARs used for "colcounts" (which is a base
	// for n-grams)
	DfltColcountVarcharSize = 255
)

// ---------------------------

type Insert struct {
	Stmt *sql.Stmt
}

func (ins *Insert) Exec(values ...any) error {
	for i, v := range values {
		if _, ok := v.(string); ok && v == "" {
			values[i] = sql.NullString{String: "", Valid: false}
		}
	}
	_, err := ins.Stmt.Exec(values...)
	return err
}

// ---------------------------

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

// ---------------------------

// importStructattrName takes either database (struct_attr) or Manatee
// (struct.name) formatted structural attribute name and returns
// the datatabase-formatted variant.
//
// Note: in APIs using vert-tagextract we expect that users can provide
// both formats in respective arguments. But please be aware that this
// behavior is possible only as long as structure names contain no underscore
// character (as otherwise, we wouldn't be able to decide where to split the name)
func importStructattrName(val string) string {
	if strings.Contains(val, ".") {
		return strings.Replace(val, ".", "_", 1)
	}
	return val
}

// ---------------------------

// BibViewConf is a sub-configuration for
// bibliographic data.
type BibViewConf struct {
	Cols   []string `json:"cols"`
	IDAttr string   `json:"idAttr"`
}

func (c *BibViewConf) IsConfigured() bool {
	return c.IDAttr != "" && len(c.Cols) > 0
}

func (c *BibViewConf) NormIDAttr() string {
	if strings.Contains(c.IDAttr, ".") {
		return strings.Replace(c.IDAttr, ".", "_", 1)
	}
	return c.IDAttr
}

func (c *BibViewConf) IDAttrElements() (string, string) {
	tmp := strings.SplitN(c.IDAttr, "_", 2)
	if len(tmp) == 1 {
		tmp = strings.SplitN(c.IDAttr, ".", 2)
	}
	if len(tmp) > 1 {
		return tmp[0], tmp[1]
	}
	return "", ""
}

// ---------------------------

type Conf struct {
	Type           string   `json:"type"`
	Name           string   `json:"name"`
	Host           string   `json:"host"`
	User           string   `json:"user"`
	Password       string   `json:"password"`
	PreconfQueries []string `json:"preconfSettings"`
}

// ---------------------------

type VertColumn struct {
	Idx   int    `json:"idx"`
	ModFn string `json:"modFn,omitempty"`

	// Role is a general "tag" specifying additional
	// usage in systems using vert-tagextract.
	// E.g. when combined with cnc-masm, we use this to
	// specify whether the column belongs to one of
	// {word, lemma, sublemma, tag, pos}
	Role string `json:"role,omitempty"`
}

func (vc VertColumn) IsUndefined() bool {
	return vc.Idx == -1
}

type VertColumns []VertColumn

func (vc VertColumns) GetByIdx(idx int) VertColumn {
	for _, v := range vc {
		if v.Idx == idx {
			return v
		}
	}
	return VertColumn{Idx: -1}
}

func (vc VertColumns) GetByRole(role string) VertColumn {
	for _, v := range vc {
		if v.Role == role {
			return v
		}
	}
	return VertColumn{Idx: -1}
}

// MaxColumn returns max index of a column
// in VertColumns. E.g. if one defines
// columns {3, 10, 7}, then 10 will be returned.
//
// Rationale: We need this because in some cases,
// it is easier to prepare slices for all the columns
// - including the ones a user does not want to export.
// E.g. for column mod functions, in case user wants
// just column 3, we create a slice {"", "", "", ""}
// so that we can theoretically apply column mod
// to any value.
func (vc VertColumns) MaxColumn() int {
	var maxc int
	for _, v := range vc {
		if v.Idx > maxc {
			maxc = v.Idx
		}
	}
	return maxc
}

// --------------------------

// DateTimeAttr represents a name of a structural attribute.
// It is defined in a way allowing enter both - Manatee format
// (e.g. 'doc.author') and database format (e.g. 'doc_author').
// It relies on the fact, that our structures even contain underscore
// so we can reliably determine where to "cut" the string.
type DateTimeAttr string

func (attr DateTimeAttr) String() string {
	return importStructattrName(string(attr))
}

func (attr DateTimeAttr) RawValue() string {
	return string(attr)
}

// ---------------------------

type Writer interface {
	DatabaseExists() bool
	Initialize(appendMode bool) error
	PrepareInsert(table string, attrs []string) (InsertOperation, error)

	// RemoveRecordsOlderThan remove all the records with date less than
	// the provided one using attr for comparison. Method should return
	// number of removed items. No matching records situation should not
	// return an error.
	RemoveRecordsOlderThan(date string, attr DateTimeAttr) (int, error)
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
func GenerateColCountNames(colCount VertColumns) []string {
	columns := make([]string, len(colCount))
	for i, v := range colCount {
		columns[i] = fmt.Sprintf("col%d", v.Idx)
	}
	return columns
}
