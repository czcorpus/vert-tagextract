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
	"github.com/czcorpus/vert-tagextract/vteconf"
	"github.com/stretchr/testify/assert"
	"testing"
)

func createDatabase() *sql.DB {
	var err error
	if db, err := sql.Open("sqlite3", ":memory:"); err == nil {
		return db
	}
	panic(err)
}

func createVTEConf() *vteconf.VTEConf {
	c := vteconf.VTEConf{
		Corpus:        "syn2010",
		VerticalFile:  "/tmp/foo",
		DBFile:        "/tmp/foo.db",
		Encoding:      "utf-8",
		AtomStructure: "doc",
		Structures:    make(map[string][]string),
	}
	c.Structures["doc"] = []string{"id", "year", "author"}
	c.Structures["p"] = []string{"num", "style"}

	bc := vteconf.BibViewConf{Cols: []string{"doc_id", "doc_author"}, IDAttr: "doc_id"}
	c.BibView = bc

	return &c
}

func containsItem(items []string, item string) bool {
	for _, v := range items {
		if item == v {
			return true
		}
	}
	return false
}

func TestGenerateColNames(t *testing.T) {
	conf := createVTEConf()
	cols := generateColNames(conf)
	assert.True(t, containsItem(cols, "doc_id"))
	assert.True(t, containsItem(cols, "doc_year"))
	assert.True(t, containsItem(cols, "doc_author"))
	assert.True(t, containsItem(cols, "p_num"))
	assert.True(t, containsItem(cols, "p_style"))
	assert.Equal(t, 5, len(cols))
}

func TestGenerateViewColDefs(t *testing.T) {
	conf := createVTEConf()
	viewCols := generateViewColDefs(&conf.BibView)
	assert.Contains(t, viewCols, "doc_id AS id")
	assert.Contains(t, viewCols, "doc_author")
	assert.Equal(t, 2, len(viewCols))
}

func TestCreateSchema(t *testing.T) {
	conf := createVTEConf()
	db := createDatabase()
	CreateSchema(db, conf)
	// cid name type notnull dflt_value pk
	res, err := db.Query("PRAGMA table_info(item)")
	if err != nil {
		panic(err)
	}
	colsSrch := make(map[string]bool)
	defer res.Close()
	for res.Next() {
		var cid string
		var name string
		var tp string
		var notnull int
		var dfltValue interface{}
		var pk int
		err := res.Scan(&cid, &name, &tp, &notnull, &dfltValue, &pk)
		if err != nil {
			panic(err)
		}
		colsSrch[name] = true
	}
	assert.Contains(t, colsSrch, "id")
	assert.Contains(t, colsSrch, "doc_id")
	assert.Contains(t, colsSrch, "doc_year")
	assert.Contains(t, colsSrch, "doc_author")
	assert.Contains(t, colsSrch, "p_num")
	assert.Contains(t, colsSrch, "p_style")
	assert.Contains(t, colsSrch, "poscount")
	assert.Contains(t, colsSrch, "wordcount")
	assert.Contains(t, colsSrch, "corpus_id")
	assert.Equal(t, 9, len(colsSrch))
}

func TestDropExisdting(t *testing.T) {
	db := createDatabase()
	db.Exec("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT")
	db.Exec("CREATE TABLE item (id INT PRIMARY KEY, name TEXT")
	db.Exec("CREATE VIEW bibliography AS SELECT * FROM item")
	DropExisting(db)

	res, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		panic(err)
	}
	assert.False(t, res.Next())
	res.Close()

	res, err = db.Query("SELECT name FROM sqlite_master WHERE type = 'view'")
	if err != nil {
		panic(err)
	}
	assert.False(t, res.Next())
	res.Close()
}

func TestCreateBibView(t *testing.T) {
	conf := createVTEConf()
	db := createDatabase()
	db.Exec("CREATE TABLE item (id INT PRIMARY KEY, doc_id TEXT, doc_year TEXT, doc_author TEXT)")
	CreateBibView(db, conf)

	res, err := db.Query("PRAGMA table_info(bibliography)")
	if err != nil {
		panic(err)
	}
	colTest := make(map[string]bool)
	defer res.Close()
	for res.Next() {
		var cid string
		var name string
		var tp string
		var notnull int
		var dfltValue interface{}
		var pk int
		err := res.Scan(&cid, &name, &tp, &notnull, &dfltValue, &pk)
		if err != nil {
			panic(err)
		}
		colTest[name] = true
	}

	assert.Contains(t, colTest, "id")
	assert.Contains(t, colTest, "doc_author")
	assert.Equal(t, 2, len(colTest))

}