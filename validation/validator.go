// Copyright 2022 Martin Zimandl <martin.zimandl@gmail.com>
// Copyright 2022 Charles University, Faculty of Arts,
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

package validation

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/czcorpus/vert-tagextract/v2/cnf"
	"github.com/czcorpus/vert-tagextract/v2/library"
	"github.com/rs/zerolog/log"

	"github.com/tomachalek/vertigo/v5"
)

var (
	ErrorTooManyParsingErrors = errors.New("too many parsing errors")
)

// Status stores some basic information about vertical file processing
type Status struct {
	Datetime       time.Time
	File           string
	ProcessedAtoms int
	ProcessedLines int
	Error          error
}

// VertValidator handles writing parsed data
// to a sqlite3 database. Parsed values are
// received pasivelly by implementing vertigo.LineProcessor
type VertValidator struct {
	vertPaths    []string
	structBuffer []string
}

// NewVertValidator is a factory function to
// instantiate proper VertValidator.
func NewVertValidator(
	conf *cnf.VTEConf,
	statusChan chan Status,
	stopChan <-chan os.Signal,
) (*VertValidator, error) {
	vertPaths, err := library.GetVerticalFiles(conf)
	if err != nil {
		return nil, err
	}
	ans := &VertValidator{
		vertPaths:    vertPaths,
		structBuffer: make([]string, 0, 20),
	}
	return ans, nil
}

// handleProcError reports a provided error err by sending it via
// statusChan and also evaluates total number of errors and in case
// it is too high (compared with a limit defined in maxNumErrors)
// it returns ErrorTooManyParsingErrors which should be considered a processing
// stop signal (but it's still up to the consumer).
func (tte *VertValidator) handleProcError(lineNum int, err error) error {
	tte.statusChan <- Status{
		Datetime:       time.Now(),
		ProcessedAtoms: tte.atomCounter,
		ProcessedLines: lineNum,
		Error:          err,
	}
	log.Printf("ERROR: Line %d: %s", lineNum, err)
	tte.errorCounter++
	if tte.errorCounter > tte.maxNumErrors {
		return ErrorTooManyParsingErrors
	}
	return nil
}

// ProcToken is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a token line is encountered.
func (tte *VertValidator) ProcToken(tk *vertigo.Token, line int, err error) error {
	select {
	case s := <-tte.stopChan:
		return fmt.Errorf("received stop signal: %s", s)
	default:
	}
	return nil
}

// ProcStruct is a part of vertigo.LineProcessor implementation.
// It si called by Vertigo parser when an opening structure tag
// is encountered.
func (tte *VertValidator) ProcStruct(st *vertigo.Structure, line int, err error) error {
	select {
	case s := <-tte.stopChan:
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
func (tte *VertValidator) ProcStructClose(st *vertigo.StructureClose, line int, err error) error {
	select {
	case s := <-tte.stopChan:
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

// Run starts the parsing and metadata extraction
// process. The method expects a proper database
// schema to be ready (see database.go for details).
// The whole process runs within a transaction which
// makes sqlite3 inserts a few orders of magnitude
// faster.
func (tte *VertValidator) Run(conf *vertigo.ParserConf) error {
	log.Print("INFO: using zero-based indexing when reporting line errors")
	log.Printf("Starting to process the vertical file %s...", conf.InputFilePath)
	parserErr := vertigo.ParseVerticalFile(conf, tte)
	if parserErr != nil {
		return fmt.Errorf("failed to parse vertical file: %s", parserErr)
	}
	return nil
}
