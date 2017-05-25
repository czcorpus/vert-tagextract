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

	"github.com/czcorpus/vert-tagextract/db/colgen"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver load
	"github.com/tomachalek/vertigo"
)

func prepareInsert(database *sql.Tx, cols []string) *sql.Stmt {
	valReplac := make([]string, len(cols))
	for i := range cols {
		valReplac[i] = "?"
	}
	ans, err := database.Prepare(fmt.Sprintf("INSERT INTO item (%s) VALUES (%s)", joinArgs(cols), joinArgs(valReplac)))
	if err != nil {
		panic(err)
	}
	return ans
}

type TTExtractor struct {
	lineCounter        int
	atomCounter        int
	tokenInAtomCounter int
	corpusID           string
	database           *sql.DB
	transaction        *sql.Tx
	insertStatement    *sql.Stmt
	stack              *structStack
	atomStruct         string
	structures         map[string][]string
	attrNames          []string
	colgenFn           colgen.AlignedColGenFn
}

func (tte *TTExtractor) ProcToken(tk *vertigo.Token) {
	tte.lineCounter++
	tte.tokenInAtomCounter++
}

func (tte *TTExtractor) ProcStructClose(st *vertigo.StructureClose) {
	tte.stack.Pop()
	tte.lineCounter++
}

func (tte *TTExtractor) acceptAttr(structName string, attrName string) bool {
	tmp := tte.structures[structName]
	for _, v := range tmp {
		if v == attrName {
			return true
		}
	}
	return false
}

func (tte *TTExtractor) ProcStruct(st *vertigo.Structure) {
	tte.stack.Push(st)
	if st.Name == tte.atomStruct {
		attrs := make(map[string]interface{})
		tte.stack.forEachAttr(func(s string, k string, v string) {
			if tte.acceptAttr(s, k) {
				attrs[fmt.Sprintf("%s_%s", s, k)] = v
			}
		})
		if tte.atomCounter == 0 {
			tte.attrNames = make([]string, len(attrs)+4)
			i := 0
			for k := range attrs {
				tte.attrNames[i] = k
				i++
			}
			tte.attrNames[i] = "wordcount"
			tte.attrNames[i+1] = "poscount"
			tte.attrNames[i+2] = "corpus_id"
			tte.attrNames[i+3] = "item_id"
			tte.insertStatement = prepareInsert(tte.transaction, tte.attrNames)
		}
		attrs["wordcount"] = 0
		attrs["poscount"] = tte.tokenInAtomCounter
		attrs["corpus_id"] = tte.corpusID
		if tte.colgenFn != nil {
			attrs["item_id"] = tte.colgenFn(attrs)
		}
		values := make([]interface{}, len(tte.attrNames))
		for i, n := range tte.attrNames {
			values[i] = attrs[n]
		}
		_, err := tte.insertStatement.Exec(values...)
		if err != nil {
			panic(err)
		}
		tte.atomCounter++
		tte.tokenInAtomCounter = 0
	}
	tte.lineCounter++
}

func (tte *TTExtractor) Run(conf *vertigo.ParserConf) {
	tte.database.Exec("PRAGMA synchronous = OFF")
	tte.database.Exec("PRAGMA journal_mode = MEMORY")
	var err error
	tte.transaction, err = tte.database.Begin()
	if err != nil {
		panic(err)
	}
	vertigo.ParseVerticalFile(conf, tte)
	tte.transaction.Commit()
}

func NewTTExtractor(database *sql.DB, corpusID string, atomStruct string, structures map[string][]string,
	colgenFn colgen.AlignedColGenFn) *TTExtractor {
	return &TTExtractor{
		database:   database,
		corpusID:   corpusID,
		atomStruct: atomStruct,
		structures: structures,
		stack:      &structStack{},
		colgenFn:   colgenFn,
	}
}
