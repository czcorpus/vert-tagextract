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
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v3/cnf"
	"github.com/czcorpus/vert-tagextract/v3/db"
	"github.com/czcorpus/vert-tagextract/v3/db/colgen"
	"github.com/czcorpus/vert-tagextract/v3/ptcount"
	"github.com/czcorpus/vert-tagextract/v3/ptcount/modders"

	_ "github.com/mattn/go-sqlite3" // sqlite3 driver load
	"github.com/tomachalek/vertigo/v6"
)

var (
	ErrorTooManyParsingErrors = errors.New("too many parsing errors")
)

func trimString(s string) string {
	limit := utf8.RuneCountInString(s)
	if limit > db.DfltColcountVarcharSize {
		limit = db.DfltColcountVarcharSize
	}
	return string([]rune(s)[:limit])
}

// Status stores some basic information about vertical file processing
type Status struct {
	Datetime       time.Time
	File           string
	ProcessedAtoms int
	ProcessedLines int
	Error          error
}

// TTExtractor handles writing parsed data
// to a sqlite3 database. Parsed values are
// received pasivelly by implementing vertigo.LineProcessor
type TTExtractor struct {
	ctx                context.Context
	atomCounter        int
	lineCounter        int
	errorCounter       int
	maxNumErrors       int
	tokenInAtomCounter int
	tokenCounter       int
	corpusID           string
	database           db.Writer
	docInsert          db.InsertOperation
	dbConf             *db.Conf
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
	columnModders      []*modders.StringTransformerChain
	colCounts          map[string]*ptcount.NgramCounter
	filter             LineFilter
	statusChan         chan<- Status
}

// NewTTExtractor is a factory function to
// instantiate proper TTExtractor.
func NewTTExtractor(
	ctx context.Context,
	database db.Writer,
	conf *cnf.VTEConf,
	colgenFn colgen.AlignedColGenFn,
	statusChan chan Status,
) (*TTExtractor, error) {
	filter, err := LoadCustomFilter(conf.Filter.Lib, conf.Filter.Fn)
	if err != nil {
		return nil, err
	}
	ans := &TTExtractor{
		ctx:              ctx,
		database:         database,
		dbConf:           &conf.DB,
		corpusID:         conf.Corpus,
		atomStruct:       conf.AtomStructure,
		atomParentStruct: conf.AtomParentStructure,
		lastAtomOpenLine: -1,
		structures:       conf.Structures,
		colgenFn:         colgenFn,
		ngramConf:        &conf.Ngrams,
		colCounts:        make(map[string]*ptcount.NgramCounter),
		columnModders:    make([]*modders.StringTransformerChain, conf.Ngrams.VertColumns.MaxColumn()+1),
		filter:           filter,
		maxNumErrors:     conf.MaxNumErrors,
		currSentence:     make([][]int, 0, 20),
		valueDict:        ptcount.NewWordDict(),
		statusChan:       statusChan,
	}

	for _, m := range conf.Ngrams.VertColumns {
		ans.columnModders[m.Idx] = modders.NewStringTransformerChain(m.ModFn)
	}
	if conf.StackStructEval {
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
// it returns ErrorTooManyParsingErrors which should be considered a processing
// stop signal (but it's still up to the consumer).
func (tte *TTExtractor) handleProcError(lineNum int, err error) error {
	tte.statusChan <- Status{
		Datetime:       time.Now(),
		ProcessedAtoms: tte.atomCounter,
		ProcessedLines: lineNum,
		Error:          err,
	}
	log.Error().Err(err).Int("lineNumber", lineNum).Msg("parsing error")
	tte.errorCounter++
	if tte.errorCounter > tte.maxNumErrors {
		return ErrorTooManyParsingErrors
	}
	return nil
}

// ProcToken is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a token line is encountered.
func (tte *TTExtractor) ProcToken(tk *vertigo.Token, line int, err error) error {
	if err != nil {
		return tte.handleProcError(line, err)
	}
	tte.lineCounter = line
	if tte.filter.Apply(tk, tte.attrAccum) {
		tte.tokenInAtomCounter++
		tte.tokenCounter = tk.Idx
		attributes := make([]int, tte.ngramConf.MaxRequiredColumn()+1)
		for _, vertCol := range tte.ngramConf.VertColumns {
			v := tk.PosAttrByIndex(vertCol.Idx)
			attributes[vertCol.Idx] = tte.valueDict.Add(tte.columnModders[vertCol.Idx].Transform(v))
		}

		tte.currSentence = append(tte.currSentence, attributes)
		if len(tte.currSentence) >= tte.ngramConf.NgramSize {
			ngram := ptcount.NewNgramCounter(tte.ngramConf.NgramSize)
			startPos := len(tte.currSentence) - tte.ngramConf.NgramSize
			for i := startPos; i < len(tte.currSentence); i++ {
				ngram.AddToken(tte.currSentence[i])
			}
			key := ngram.UniqueID()
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
	case s := <-tte.ctx.Done():
		return fmt.Errorf("received stop signal: %s", s)
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
			tte.currAtomAttrs = attrs
			tte.atomCounter++
			if tte.colgenFn != nil {
				var err4 error
				attrs["item_id"], err4 = tte.colgenFn(attrs)
				if err4 != nil {
					return tte.handleProcError(line, err4)
				}
			}

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
	case s := <-tte.ctx.Done():
		return fmt.Errorf("received stop signal: %s", s)
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
		if tte.currAtomAttrs == nil {
			return fmt.Errorf(
				"currAtomAttrs not initialized for accum. structure: %s, curr. elm.: %s, line: %d",
				st.Name, accumItem.elm.Name, line)
		}
		tte.currAtomAttrs["poscount"] = tte.tokenInAtomCounter
		values := make([]any, len(tte.attrNames))
		for i, n := range tte.attrNames {
			if tte.currAtomAttrs[n] != nil {
				values[i] = tte.currAtomAttrs[n]

			} else {
				values[i] = "" // liveattrs plug-in does not like NULLs
			}
		}
		err := tte.docInsert.Exec(values...)
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
	attrNames := make([]string, 0, tte.calcNumAttrs()+4)
	for s, items := range tte.structures {
		for _, item := range items {
			attrNames = append(attrNames, fmt.Sprintf("%s_%s", s, item))
		}
	}
	attrNames = append(attrNames, "wordcount")
	attrNames = append(attrNames, "poscount")
	attrNames = append(attrNames, "corpus_id")
	if tte.colgenFn != nil {
		attrNames = append(attrNames, "item_id")
	}
	return attrNames
}

func (tte *TTExtractor) generateHashID(ng *ptcount.NgramCounter) string {
	hasher := sha1.New()
	for _, vc := range tte.ngramConf.VertColumns {
		hasher.Write([]byte(ng.ColumnNgram(vc.Idx, tte.valueDict)))
	}
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (tte *TTExtractor) insertCounts() error {
	colItems := append(
		db.GenerateColCountNames(tte.ngramConf.VertColumns),
		"corpus_id", "count", "arf", "hash_id")
	ins, err := tte.database.PrepareInsert("colcounts", colItems)
	if err != nil {
		return nil
	}
	i := 0
	for _, count := range tte.colCounts {
		select {
		case s := <-tte.ctx.Done():
			return fmt.Errorf("received stop signal: %s", s)
		default:
		}

		args := make([]interface{}, len(tte.ngramConf.VertColumns)+4)
		for i, vc := range tte.ngramConf.VertColumns {
			args[i] = count.ColumnNgram(vc.Idx, tte.valueDict)
		}

		numCol := len(tte.ngramConf.VertColumns)
		args[numCol] = tte.corpusID
		args[numCol+1] = count.Count()
		if count.HasARF() {
			args[numCol+2] = count.ARF().ARF

		} else {
			args[numCol+2] = -1
		}
		args[numCol+3] = tte.generateHashID(count)
		err = ins.Exec(args...)
		if err != nil {
			return err
		}

		if i > 0 && i%1000 == 0 {
			tte.statusChan <- Status{
				Datetime:       time.Now(),
				ProcessedAtoms: tte.atomCounter,
				ProcessedLines: tte.lineCounter,
			}
			if i%100000 == 0 {
				log.Info().
					Int("numProcessed", i).
					Msg("next chunk of records processed")
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
	log.Info().Msg("using zero-based indexing when reporting line errors")
	log.Info().Str("file", conf.InputFilePath).Msg("Starting to process vertical file")
	tte.attrNames = tte.generateAttrList()
	var err error
	tte.docInsert, err = tte.database.PrepareInsert("liveattrs_entry", tte.attrNames)
	if err != nil {
		return err
	}
	parserErr := vertigo.ParseVerticalFile(tte.ctx, conf, tte)
	if parserErr != nil {
		tte.database.Rollback()
		tte.statusChan <- Status{
			Datetime:       time.Now(),
			Error:          parserErr,
			ProcessedAtoms: tte.atomCounter,
			ProcessedLines: -1,
		}
		return fmt.Errorf("failed to parse vertical file: %s", parserErr)
	}
	if len(tte.ngramConf.VertColumns) > 0 {
		if tte.ngramConf.CalcARF {
			log.Info().
				Msg("calculating ARF (processing the vertical again)")
			arfCalc := ptcount.NewARFCalculator(
				tte.GetColCounts(),
				tte.ngramConf,
				tte.GetNumTokens(),
				tte.columnModders,
				tte.WordDict(),
				tte.atomStruct,
			)
			parserErr := vertigo.ParseVerticalFile(tte.ctx, conf, arfCalc)
			if parserErr != nil {
				return fmt.Errorf("ERROR: %s", parserErr)
			}
			arfCalc.Finalize()
		}
		log.Info().Msg("Saving defined positional attributes counts into the database")
		err = tte.insertCounts()
		if err != nil {
			return err
		}
	}
	return nil
}
