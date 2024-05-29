// Copyright 2021 Martin Zimandl <martin.zimandl@gmail.com>
// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2024 Charles University, Faculty of Arts,
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

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var (
	tstPos = []string{
		"X", "INTJ", "PRON", "NOUN", "PUNCT",
		"ADP", "SCONJ", "VERB", "DET", "PROPN",
		"ADV", "PART", "AUX", "ADJ", "CCONJ", "NUM",
	}

	tstFeat = []string{
		"Polarity", "Case", "Degree", "Mood", "Person",
		"Poss", "PronType", "ExtPos", "Reflex", "NumForm",
		"Foreign", "VerbForm", "Tense", "Abbr", "NumType",
		"Definite", "Gender", "Typo", "Number", "Voice",
	}
)

type analyzer struct {
	posTst    map[string]bool
	featTst   map[string]bool
	numMiss   int64
	procLines int64
	lastErr   string
	nullMode  bool
}

func (a *analyzer) SetNewLine() {
	a.procLines++
}

func (a *analyzer) AddError() {
	a.numMiss++
}

func (a *analyzer) AddNamedError(msg string) {
	a.numMiss += 10 // named error has higher weight than e.g. an unknown feature
	a.lastErr = msg
}

func (a *analyzer) LastErr() string {
	return a.lastErr
}

func (a *analyzer) AddFeat(name string) {
	_, ok := a.featTst[name]
	if !ok {
		fmt.Println("@@@ unknown feat: ", name)
		a.numMiss++
	}
}

func (a *analyzer) AddPos(value string) {
	_, ok := a.posTst[value]
	if !ok {
		a.numMiss++
	}
}

func (a *analyzer) TooMuchErrors() bool {
	return !a.nullMode && a.procLines > 1000 && a.numMiss > 100
}

func newAnalyzer(nullMode bool) *analyzer {
	a := &analyzer{
		posTst:   make(map[string]bool),
		featTst:  make(map[string]bool),
		nullMode: nullMode,
	}
	for _, v := range tstPos {
		a.posTst[v] = true
	}
	for _, v := range tstFeat {
		a.featTst[v] = true
	}
	return a
}

func showSelectedFeats(path string, posIdx, featIdx int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	rdr := bufio.NewScanner(f)
	var i int
	printMsg("\nSelected attributes preview:\n\n")
	printMsg("PoS \t| Feat\n")
	printMsg("----------------")
	for rdr.Scan() {
		line := rdr.Text()
		if !strings.HasPrefix(line, "<") { // a line with structure definition
			tmp := strings.Split(line, "\t")
			printMsg("%s\t| %s\n", tmp[posIdx], tmp[featIdx])
			i++
		}
		if i > 20 {
			break
		}
	}
	printMsg("---------------------")
	return nil
}
