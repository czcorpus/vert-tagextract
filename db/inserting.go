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
	"log"

	"github.com/czcorpus/vert-tagextract/db/colgen"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver load
	"github.com/tomachalek/vertigo"
)

// prepareInsert creates a prepared statement for the INSERT
// operation.
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

// TTExtractor handles writing parsed data
// to a sqlite3 database. Parsed values are
// received pasivelly by implementing vertigo.LineProcessor
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

// ProcToken is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a token line is encountered.
func (tte *TTExtractor) ProcToken(tk *vertigo.Token) {
	tte.lineCounter++
	tte.tokenInAtomCounter++
}

// ProcStructClose is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a closing structure tag is
// encountered.
func (tte *TTExtractor) ProcStructClose(st *vertigo.StructureClose) {
	tte.stack.Pop()
	tte.lineCounter++
}

// acceptAttr tests whether a structural attribute
// [structName].[attrName] is configured (see _example/*.json) to be imported
func (tte *TTExtractor) acceptAttr(structName string, attrName string) bool {
	tmp := tte.structures[structName]
	for _, v := range tmp {
		if v == attrName {
			return true
		}
	}
	return false
}

// ProcStruct is a part of vertigo.LineProcessor implementation.
// It si called by Vertigo parser when an opening structure tag
// is encountered.
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
			if tte.colgenFn != nil {
				tte.attrNames[i+3] = "item_id"

			} else {
				tte.attrNames = tte.attrNames[:i+3]
			}
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
			if attrs[n] != nil {
				values[i] = attrs[n]

			} else {
				values[i] = "" // liveattrs plug-in does not like NULLs
			}
		}
		_, err := tte.insertStatement.Exec(values...)
		if err != nil {
			log.Fatalf("Failed to insert data: %s", err)
		}
		tte.atomCounter++
		tte.tokenInAtomCounter = 0
	}
	tte.lineCounter++
}

// Run starts the parsing and metadata extraction
// process. The method expects a proper database
// schema to be ready (see database.go for details).
// The whole process runs within a transaction which
// makes sqlite3 inserts a few orders of magnitude
// faster.
func (tte *TTExtractor) Run(conf *vertigo.ParserConf) {
	log.Print("Starting to process the vertical file...")
	tte.database.Exec("PRAGMA synchronous = OFF")
	tte.database.Exec("PRAGMA journal_mode = MEMORY")
	var err error
	tte.transaction, err = tte.database.Begin()
	if err != nil {
		log.Fatalf("Failed to start a database transaction: %s", err)
	}
	parserErr := vertigo.ParseVerticalFile(conf, tte)
	if parserErr != nil {
		tte.transaction.Rollback()
		log.Fatalf("Failed to parse vertical file: %s", parserErr)

	} else {
		tte.transaction.Commit()
		log.Print("...DONE")
	}
}

// NewTTExtractor is a factory function to
// instantiate proper TTExtractor.
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
