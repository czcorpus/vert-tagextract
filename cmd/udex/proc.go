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
		"ADJ", "ADP", "ADV", "ASP", "AUX", "BS", "CCONJ", "COMP", "CONJ_CORD", "CONJ_SUB",
		"DEF", "DET", "FOC", "FUT", "GEN", "GEN_DEF", "GEN_PRON", "HEMM", "INT", "INTJ",
		"KIEN", "KN", "LIL", "LIL_DEF", "LIL_PRON", "NEG", "NOUN", "NOUN_PROP", "NSE",
		"NUM", "NUM_CRD", "NUM_FRC", "NUM_ORD", "NUM_WHD", "PART", "PART_ACT", "PART_PASS",
		"PDHEDP", "PDOENP", "PEMP", "PREP", "PREP_DEF", "PREP_PRON", "PROG", "PRON", "PRON_DEM",
		"PRON_DEM_DEF", "PRON_INDEF", "PRON_INT", "PRON_PERS", "PRON_PERS_NEG", "PRON_REC",
		"PRON_REF", "PROPN", "PTEDP", "PTENP", "PUNCT", "PV", "QUAN", "RS", "SCONJ", "SVS",
		"SYM", "UPI", "UPO", "UPS", "VERB", "VERB_PSEU", "VTHOO", "VTUOA", "VTUOM", "X",
		"X_ABV", "X_BOR", "X_DIG", "X_ENG", "X_FOR", "X_PUN", "ZE", "ZM",
	}

	tstFeat = []string{
		"Abbr", "AdjType", "AdpType", "Animacy", "Aspect", "Case", "Clitic", "ConjType",
		"Definite", "Degree", "ExtPos", "Foreign", "Gender", "Gender[psor]", "Hyph", "Mood",
		"NameType", "NumForm", "NumType", "NumValue", "Number", "Number[psor]", "PartType",
		"Person", "Person[psor]", "Polarity", "Poss", "PrepCase", "PronType", "Reflex", "Style",
		"Subcat", "Tense", "Typo", "Variant", "VerbForm", "VerbType", "Voice",
	}
)

type analyzer struct {
	posTst       map[string]bool
	featTst      map[string]bool
	numMiss      int64
	procLines    int64
	lastErr      string
	nullMode     bool
	maxNumErrors int64
}

func (a *analyzer) SetNewLine() {
	a.procLines++
}

func (a *analyzer) AddError() {
	a.numMiss++
}

func (a *analyzer) AddNamedError(msg string) {
	printMsg(msg)
	a.numMiss += 10 // named error has higher weight than e.g. an unknown feature
	a.lastErr = msg
}

func (a *analyzer) LastErr() string {
	return a.lastErr
}

func (a *analyzer) AddFeat(name string) {
	_, ok := a.featTst[name]
	if !ok {
		a.lastErr = fmt.Sprintf("@@@ unknown feat: %s", name)
		printMsg(a.lastErr)
		a.numMiss++
	}
}

func (a *analyzer) AddPos(value string) {
	if value == "" {
		printMsg("ignoring empty PoS")
		return
	}
	_, ok := a.posTst[value]
	if !ok {
		a.lastErr = fmt.Sprintf("@@@ unknown PoS: %s", value)
		printMsg(a.lastErr)
		a.numMiss++
	}
}

func (a *analyzer) TooManyErrors() bool {
	return !a.nullMode && a.procLines > 1000 && a.numMiss > a.maxNumErrors
}

func newAnalyzer(nullMode bool, maxNumErrors int64) *analyzer {
	a := &analyzer{
		posTst:       make(map[string]bool),
		featTst:      make(map[string]bool),
		nullMode:     nullMode,
		maxNumErrors: maxNumErrors,
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
			printMsg("%s\t| %s", tmp[posIdx], tmp[featIdx])
			i++
		}
		if i > 20 {
			break
		}
	}
	printMsg("---------------------")
	return nil
}
