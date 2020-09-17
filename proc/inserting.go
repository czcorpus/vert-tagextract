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

package proc

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/czcorpus/vert-tagextract/cnf"
	"github.com/czcorpus/vert-tagextract/db"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/ptcount"
	"github.com/czcorpus/vert-tagextract/ptcount/modders"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver load
	"github.com/tomachalek/vertigo/v5"
)

// Status stores some basic information about vertical file processing
type Status struct {
	Datetime       time.Time
	File           string
	ProcessedAtoms int
	ProcessedLines int
	Error          error
}

// TTEConfProvider defines an object able to
// provide configuration data for TTExtractor factory.
type TTEConfProvider interface {
	GetCorpus() string
	GetAtomStructure() string
	GetAtomParentStructure() string
	GetStackStructEval() bool
	GetMaxNumErrors() int
	GetStructures() map[string][]string
	GetNgrams() *cnf.NgramConf
	HasConfiguredFilter() bool
	GetFilterLib() string
	GetFilterFn() string
	GetDbConfSettings() []string
}

// TTExtractor handles writing parsed data
// to a sqlite3 database. Parsed values are
// received pasivelly by implementing vertigo.LineProcessor
type TTExtractor struct {
	atomCounter        int
	lineCounter        int
	errorCounter       int
	maxNumErrors       int
	tokenInAtomCounter int
	tokenCounter       int
	corpusID           string
	database           *sql.DB
	dbConf             []string
	transaction        *sql.Tx
	docInsert          *sql.Stmt
	attrAccum          AttrAccumulator
	atomStruct         string
	atomParentStruct   string
	lastAtomOpenLine   int
	structures         map[string][]string
	attrNames          []string
	colgenFn           colgen.AlignedColGenFn
	currAtomAttrs      map[string]interface{}
	ngramConf          *cnf.NgramConf
	currSentence       [][]int
	valueDict          *ptcount.WordDict
	columnModders      []*modders.ModderChain
	colCounts          map[string]*ptcount.NgramCounter
	filter             LineFilter
	stopChan           <-chan os.Signal
	statusChan         chan<- Status
}

// NewTTExtractor is a factory function to
// instantiate proper TTExtractor.
func NewTTExtractor(database *sql.DB, conf TTEConfProvider,
	colgenFn colgen.AlignedColGenFn, statusChan chan Status, stopChan <-chan os.Signal) (*TTExtractor, error) {
	filter, err := LoadCustomFilter(conf.GetFilterLib(), conf.GetFilterFn())
	if err != nil {
		return nil, err
	}
	ans := &TTExtractor{
		database:         database,
		dbConf:           conf.GetDbConfSettings(),
		corpusID:         conf.GetCorpus(),
		atomStruct:       conf.GetAtomStructure(),
		atomParentStruct: conf.GetAtomParentStructure(),
		lastAtomOpenLine: -1,
		structures:       conf.GetStructures(),
		colgenFn:         colgenFn,
		ngramConf:        conf.GetNgrams(),
		colCounts:        make(map[string]*ptcount.NgramCounter),
		columnModders:    make([]*modders.ModderChain, len(conf.GetNgrams().AttrColumns)),
		filter:           filter,
		maxNumErrors:     conf.GetMaxNumErrors(),
		currSentence:     make([][]int, 0, 20),
		valueDict:        ptcount.NewWordDict(),
		statusChan:       statusChan,
		stopChan:         stopChan,
	}

	for i, m := range conf.GetNgrams().ColumnMods {
		values := strings.Split(m, ":")
		if len(values) > 0 {
			mod := make([]modders.Modder, 0, len(values))
			for _, v := range values {
				mod = append(mod, modders.ModderFactory(v))
			}
			ans.columnModders[i] = modders.NewModderChain(mod)
		}
	}
	if conf.GetStackStructEval() {
		ans.attrAccum = newStructStack()

	} else {
		ans.attrAccum = newDefaultAccum()
	}

	return ans, nil
}

func (tte *TTExtractor) GetNumTokens() int {
	return tte.tokenCounter
}

func (tte *TTExtractor) WordDict() *ptcount.WordDict {
	return tte.valueDict
}

func (tte *TTExtractor) GetColCounts() map[string]*ptcount.NgramCounter {
	return tte.colCounts
}

// handleProcError reports a provided error err by sending it via
// statusChan and also evaluates total number of errors and in case
// it is too high (compared with a limit defined in maxNumErrors)
// it returns a new error which should be considered a processing
// stop signal.
func (tte *TTExtractor) handleProcError(lineNum int, err error) error {
	tte.statusChan <- Status{
		Datetime:       time.Now(),
		ProcessedAtoms: tte.atomCounter,
		ProcessedLines: lineNum,
		Error:          err,
	}
	log.Printf("ERROR: Line %d: %s", lineNum, err)
	tte.errorCounter++
	if tte.errorCounter > tte.maxNumErrors {
		return fmt.Errorf("FATAL: too many errors")
	}
	return nil
}

// ProcToken is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a token line is encountered.
func (tte *TTExtractor) ProcToken(tk *vertigo.Token, line int, err error) error {
	select {
	case s := <-tte.stopChan:
		return fmt.Errorf("Received stop signal: %s", s)
	default:
	}
	if err != nil {
		return tte.handleProcError(line, err)
	}
	tte.lineCounter = line
	if tte.filter.Apply(tk, tte.attrAccum) {
		tte.tokenInAtomCounter++
		tte.tokenCounter = tk.Idx

		attributes := make([]int, len(tte.ngramConf.AttrColumns))
		for i, idx := range tte.ngramConf.AttrColumns {
			v := tk.PosAttrByIndex(idx)
			attributes[i] = tte.valueDict.Add(tte.columnModders[i].Mod(v))
		}

		tte.currSentence = append(tte.currSentence, attributes)
		if len(tte.currSentence) >= tte.ngramConf.NgramSize {
			ngram := ptcount.NewNgramCounter(tte.ngramConf.NgramSize)
			startPos := len(tte.currSentence) - tte.ngramConf.NgramSize
			for i := startPos; i < len(tte.currSentence); i++ {
				ngram.AddToken(tte.currSentence[i])
			}
			key := ngram.UniqueID(tte.ngramConf.UniqKeyColumns)
			cnt, ok := tte.colCounts[key]
			if !ok {
				tte.colCounts[key] = ngram

			} else {
				cnt.IncCount()
			}
		}
	}
	if line%1000 == 0 {
		tte.statusChan <- Status{
			Datetime:       time.Now(),
			ProcessedAtoms: tte.atomCounter,
			ProcessedLines: line,
		}
	}
	return nil
}

func (tte *TTExtractor) getCurrentAccumAttrs() map[string]interface{} {
	attrs := make(map[string]interface{})
	tte.attrAccum.ForEachAttr(func(s string, k string, v string) bool {
		if tte.acceptAttr(s, k) {
			attrs[fmt.Sprintf("%s_%s", s, k)] = v
		}
		return true
	})
	return attrs
}

// ProcStruct is a part of vertigo.LineProcessor implementation.
// It si called by Vertigo parser when an opening structure tag
// is encountered.
func (tte *TTExtractor) ProcStruct(st *vertigo.Structure, line int, err error) error {
	select {
	case s := <-tte.stopChan:
		return fmt.Errorf("Received stop signal: %s", s)
	default:
	}
	if err != nil { // error from the Vertigo parser
		return tte.handleProcError(line, err)
	}
	tte.lineCounter = line
	err2 := tte.attrAccum.begin(line, st)
	if err2 != nil {
		return tte.handleProcError(line, err2)
	}
	if st.IsEmpty {
		_, err3 := tte.attrAccum.end(line, st.Name)
		if err3 != nil {
			return tte.handleProcError(line, err3)
		}
	}

	if st != nil {
		if st.Name == tte.atomStruct {
			tte.lastAtomOpenLine = line
			tte.tokenInAtomCounter = 0
			attrs := tte.getCurrentAccumAttrs()
			attrs["wordcount"] = 0 // This value is currently unused
			attrs["poscount"] = 0  // This value is updated once we hit the closing tag
			attrs["corpus_id"] = tte.corpusID
			if tte.colgenFn != nil {
				var err4 error
				attrs["item_id"], err4 = tte.colgenFn(attrs)
				if err4 != nil {
					return tte.handleProcError(line, err4)
				}
			}
			tte.currAtomAttrs = attrs
			tte.atomCounter++

		} else if st.Name == tte.atomParentStruct {
			attrs := tte.getCurrentAccumAttrs()
			attrs["wordcount"] = 0 // This value is currently unused
			attrs["poscount"] = 0  // This value is updated once we hit the closing tag
			attrs["corpus_id"] = tte.corpusID
			if tte.colgenFn != nil {
				var err5 error
				attrs["item_id"], err5 = tte.colgenFn(attrs)
				if err5 != nil {
					return tte.handleProcError(line, err5)
				}
			}
			tte.currAtomAttrs = attrs
		}
	}
	if line%1000 == 0 {
		tte.statusChan <- Status{
			Datetime:       time.Now(),
			ProcessedAtoms: tte.atomCounter,
			ProcessedLines: line,
		}
	}
	return nil
}

// ProcStructClose is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a closing structure tag is
// encountered.
func (tte *TTExtractor) ProcStructClose(st *vertigo.StructureClose, line int, err error) error {
	select {
	case s := <-tte.stopChan:
		return fmt.Errorf("Received stop signal: %s", s)
	default:
	}
	if err != nil { // error from the Vertigo parser
		return tte.handleProcError(line, err)
	}
	accumItem, err2 := tte.attrAccum.end(line, st.Name)
	if err2 != nil {
		return tte.handleProcError(line, err2)
	}
	tte.lineCounter = line
	if accumItem.elm.Name == tte.atomStruct ||
		accumItem.elm.Name == tte.atomParentStruct && tte.lastAtomOpenLine < accumItem.lineOpen {

		tte.currAtomAttrs["poscount"] = tte.tokenInAtomCounter
		values := make([]interface{}, len(tte.attrNames))
		for i, n := range tte.attrNames {
			if tte.currAtomAttrs[n] != nil {
				values[i] = tte.currAtomAttrs[n]

			} else {
				values[i] = "" // liveattrs plug-in does not like NULLs
			}
		}
		_, err := tte.docInsert.Exec(values...)
		if err != nil {
			return tte.handleProcError(line, err)

		}
		tte.currAtomAttrs = make(map[string]interface{})

		// also reset the current sentence
		tte.currSentence = tte.currSentence[:0]
	}
	if line%1000 == 0 {
		tte.statusChan <- Status{
			Datetime:       time.Now(),
			ProcessedAtoms: tte.atomCounter,
			ProcessedLines: line,
		}
	}
	return nil
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

func (tte *TTExtractor) calcNumAttrs() int {
	ans := 0
	for _, items := range tte.structures {
		ans += len(items)
	}
	return ans
}

func (tte *TTExtractor) generateAttrList() []string {
	attrNames := make([]string, tte.calcNumAttrs()+4)
	i := 0
	for s, items := range tte.structures {
		for _, item := range items {
			attrNames[i] = fmt.Sprintf("%s_%s", s, item)
			i++
		}
	}
	attrNames[i] = "wordcount"
	attrNames[i+1] = "poscount"
	attrNames[i+2] = "corpus_id"
	if tte.colgenFn != nil {
		attrNames[i+3] = "item_id"

	} else {
		attrNames = attrNames[:i+3]
	}
	return attrNames
}

func (tte *TTExtractor) insertCounts() error {
	colItems := append(db.GenerateColCountNames(tte.ngramConf.AttrColumns), "corpus_id", "count", "arf")
	ins, err := db.PrepareInsert(tte.transaction, "colcounts", colItems)
	if err != nil {
		return nil
	}
	i := 0
	for _, count := range tte.colCounts {
		select {
		case s := <-tte.stopChan:
			return fmt.Errorf("Received stop signal: %s", s)
		default:
		}
		args := make([]interface{}, count.Width()+3)
		count.ForEachAttr(tte.valueDict, func(v string, i int) {
			args[i] = v
		})
		args[count.Width()] = tte.corpusID
		args[count.Width()+1] = count.Count()
		if count.HasARF() {
			args[count.Width()+2] = count.ARF().ARF

		} else {
			args[count.Width()+2] = -1
		}
		ins.Exec(args...)

		if i > 0 && i%1000 == 0 {
			tte.statusChan <- Status{
				Datetime:       time.Now(),
				ProcessedAtoms: tte.atomCounter,
				ProcessedLines: tte.lineCounter,
			}
			if i%100000 == 0 {
				log.Printf("... written %d records", i)
			}
		}
		i++
	}
	return nil
}

// Run starts the parsing and metadata extraction
// process. The method expects a proper database
// schema to be ready (see database.go for details).
// The whole process runs within a transaction which
// makes sqlite3 inserts a few orders of magnitude
// faster.
func (tte *TTExtractor) Run(conf *vertigo.ParserConf) error {
	log.Print("INFO: using zero-based indexing when reporting line errors")
	log.Printf("Starting to process the vertical file %s...", conf.InputFilePath)
	var dbConf []string
	if len(tte.dbConf) > 0 {
		dbConf = tte.dbConf

	} else {
		log.Print("INFO: no database configuration found, using default (see below)")
		dbConf = []string{
			"PRAGMA synchronous = OFF",
			"PRAGMA journal_mode = MEMORY",
		}
	}
	for _, cnf := range dbConf {
		log.Printf("INFO: Applying %s", cnf)
		tte.database.Exec(cnf)
	}

	var err error
	tte.transaction, err = tte.database.Begin()
	if err != nil {
		return fmt.Errorf("Failed to start a database transaction: %s", err)
	}

	tte.attrNames = tte.generateAttrList()
	tte.docInsert, err = db.PrepareInsert(tte.transaction, "item", tte.attrNames)
	if err != nil {
		return err
	}
	parserErr := vertigo.ParseVerticalFile(conf, tte)
	if parserErr != nil {
		tte.transaction.Rollback()
		tte.statusChan <- Status{
			Datetime:       time.Now(),
			Error:          parserErr,
			ProcessedAtoms: tte.atomCounter,
			ProcessedLines: -1,
		}
		return fmt.Errorf("Failed to parse vertical file: %s", parserErr)
	}
	if len(tte.ngramConf.AttrColumns) > 0 {
		if tte.ngramConf.CalcARF {
			log.Print("####### 2nd run - calculating ARF ###################")
			arfCalc := ptcount.NewARFCalculator(
				tte.GetColCounts(),
				tte.ngramConf,
				tte.GetNumTokens(),
				tte.columnModders,
				tte.WordDict(),
				tte.atomStruct,
			)
			parserErr := vertigo.ParseVerticalFile(conf, arfCalc)
			if parserErr != nil {
				return fmt.Errorf("ERROR: %s", parserErr)
			}
			arfCalc.Finalize()
		}
		log.Print("Saving defined positional attributes counts into the database...")
		err = tte.insertCounts()
		if err != nil {
			return err
		}
	}
	err = tte.transaction.Commit()
	if err != nil {
		return fmt.Errorf("Failed to commit database transaction: %s", err)
	}
	log.Print("...DONE")
	return nil
}
