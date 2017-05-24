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

	_ "github.com/mattn/go-sqlite3"
	"github.com/tomachalek/vertigo"
)

func PrepareInsert(database *sql.DB, cols map[string]string, posCount int, wordCount int, itemId string) *sql.Stmt {
	colNames := make([]string, len(cols))
	colValues := make([]string, len(cols))
	valReplac := make([]string, len(cols))
	i := 0
	for k, v := range cols {
		colNames[i] = k
		colValues[i] = v
		valReplac[i] = "?"
		i++
	}
	ans, err := database.Prepare(fmt.Sprintf("INSERT INTO item (%s) VALUES (%s)", joinArgs(colNames), joinArgs(valReplac)))
	if err != nil {
		panic(err)
	}
	return ans
}

type TTExtractor struct {
	lineCounter     int
	database        *sql.DB
	insertStatement *sql.Stmt
	stack           structStack
	atomStruct      string
	structures      map[string][]string
}

func (tte *TTExtractor) ProcToken(tk *vertigo.Token) {
	tte.lineCounter++
}

func (tte *TTExtractor) ProcStructClose(st *vertigo.StructureClose) {
	tte.stack.Pop()
	tte.lineCounter++
}

func (tte *TTExtractor) ProcStruct(st *vertigo.Structure) {
	if st.Name == tte.atomStruct {
		fmt.Println(">>> SAVE STRUCT: ", st.Name, st.Attrs)
		tte.stack.GoThroughAttrs(func(s string, k string, v string) {
			fmt.Printf("[%s.%s] --> %s\n", s, k, v)
		})
	}
	tte.stack.Push(st)
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
	}
}
