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

	_ "github.com/mattn/go-sqlite3" // sqlite3 driver load
	"github.com/tomachalek/vertigo"
)

func prepareInsert(database *sql.DB, cols []string) *sql.Stmt {
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
	database           *sql.DB
	insertStatement    *sql.Stmt
	stack              *structStack
	atomStruct         string
	structures         map[string][]string
	attrNames          []string
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
		tte.stack.GoThroughAttrs(func(s string, k string, v string) {
			if tte.acceptAttr(s, k) {
				attrs[fmt.Sprintf("%s_%s", s, k)] = v
			}
		})
		if tte.atomCounter == 0 {
			tte.attrNames = make([]string, len(attrs)+2)
			i := 0
			for k := range attrs {
				tte.attrNames[i] = k
				i++
			}
			tte.attrNames[i] = "wordcount"
			tte.attrNames[i+1] = "poscount"
			tte.insertStatement = prepareInsert(tte.database, tte.attrNames)
		}
		attrs["wordcount"] = 0
		attrs["poscount"] = tte.tokenInAtomCounter
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
	vertigo.ParseVerticalFile(conf, tte)
}

func NewTTExtractor(database *sql.DB, atomStruct string, structures map[string][]string) *TTExtractor {
	return &TTExtractor{
		database:   database,
		atomStruct: atomStruct,
		structures: structures,
		stack:      &structStack{},
	}
}
