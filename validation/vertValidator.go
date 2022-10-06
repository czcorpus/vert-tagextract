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
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"

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

// VertValidator handles vertical validation. Parsed values are
// received pasivelly by implementing vertigo.LineProcessor
type VertValidator struct {
	vertPaths   []string
	openStructs []*vertigo.Structure
	strict      bool
	stopChan    <-chan os.Signal
}

// NewVertValidator is a factory function to
// instantiate proper VertValidator.
func NewVertValidator(
	vertPaths []string,
	strict bool,
	stopChan <-chan os.Signal,
) (*VertValidator, error) {
	ans := &VertValidator{
		vertPaths:   vertPaths,
		openStructs: make([]*vertigo.Structure, 0, 20),
		strict:      strict,
		stopChan:    stopChan,
	}
	return ans, nil
}

// ProcToken is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a token line is encountered.
func (vv *VertValidator) ProcToken(tk *vertigo.Token, line int, err error) error {
	select {
	case s := <-vv.stopChan:
		return fmt.Errorf("received stop signal: %s", s)
	default:
	}
	return nil
}

// ProcStruct is a part of vertigo.LineProcessor implementation.
// It si called by Vertigo parser when an opening structure tag
// is encountered.
func (vv *VertValidator) ProcStruct(st *vertigo.Structure, line int, err error) error {
	select {
	case s := <-vv.stopChan:
		return fmt.Errorf("received stop signal: %s", s)
	default:
	}
	if err != nil {
		return err
	}
	if !st.IsEmpty {
		for _, v := range vv.openStructs {
			if v.Name == st.Name {
				return fmt.Errorf("elements can not contain itself on line %d, structure %s is already opened", line, st.Name)
			}
		}
		vv.openStructs = append(vv.openStructs, st)
	}
	return nil
}

// ProcStructClose is a part of vertigo.LineProcessor implementation.
// It is called by Vertigo parser when a closing structure tag is
// encountered.
func (vv *VertValidator) ProcStructClose(st *vertigo.StructureClose, line int, err error) error {
	select {
	case s := <-vv.stopChan:
		return fmt.Errorf("received stop signal: %s", s)
	default:
	}
	if err != nil {
		return err
	}

	if vv.strict {
		// closing tag should correspond to last opened tag in stack
		if st.Name == vv.openStructs[len(vv.openStructs)-1].Name {
			vv.openStructs = vv.openStructs[:len(vv.openStructs)-1]
		} else {
			return fmt.Errorf("invalid closing element `%s` on line %d, expecting element `%s`", st.Name, line, vv.openStructs[len(vv.openStructs)-1].Name)
		}

	} else {
		// opening tag should be somewhere in the stack
		// all opened elements after it will be discarded
		i := len(vv.openStructs) - 1
		for i >= 0 {
			if vv.openStructs[i].Name == st.Name {
				vv.openStructs = vv.openStructs[:i]
				break
			}
			if i > 0 {
				i--
			} else {
				return fmt.Errorf("missing opening tag for element `%s` on line %d", st.Name, line)
			}
		}
	}

	return nil
}

// Run vertical validation
func (vv *VertValidator) Run(conf *vertigo.ParserConf) error {
	log.Print("INFO: using zero-based indexing when reporting line errors")
	log.Printf("Starting to process the vertical file %s...", conf.InputFilePath)
	parserErr := vertigo.ParseVerticalFile(conf, vv)
	if parserErr != nil {
		return fmt.Errorf("failed to parse vertical file: %s", parserErr)
	}
	return nil
}
