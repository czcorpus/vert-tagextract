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

package cnf

import (
	"encoding/json"
	"io/ioutil"
)

// SelfJoinConf contains information about aligned
// structural attributes (e.g. sentences from two
// languages).
type SelfJoinConf struct {
	ArgColumns  []string `json:"argColumns"`
	GeneratorFn string   `json:"generatorFn"`
}

// BibViewConf is a sub-configuration for
// bibliographic data.
type BibViewConf struct {
	Cols   []string `json:"cols"`
	IDAttr string   `json:"idAttr"`
}

// FilterConf specifies a plug-in containing
// a compatible filter (see LineFilter interface).
type FilterConf struct {
	Lib string `json:"lib"`
	Fn  string `json:"fn"`
}

// NgramConf configures positional attributes (referred by their
// column position) we want to store and count as n-grams. This can
// be used to extract all the unique PoS tags or frequency information
// about words/lemmas.
type NgramConf struct {
	AttrColumns    []int    `json:"attrColumns"`
	ColumnMods     []string `json:"columnMods"`
	NgramSize      int      `json:"ngramSize"`
	UniqKeyColumns []int    `json:"uniqKeyColumns"`
	CalcARF        bool     `json:"calcARF"`
}

// VTEConf holds configuration for a concrete
// data extraction task.
type VTEConf struct {
	Corpus              string `json:"corpus"`
	AtomStructure       string `json:"atomStructure"`
	AtomParentStructure string `json:"atomParentStructure"`
	StackStructEval     bool   `json:"stackStructEval"`

	// MaxNumErrors if reached then the process stops
	MaxNumErrors int                 `json:"maxNumErrors"`
	Structures   map[string][]string `json:"structures"`

	// Ngrams - see NgramConf
	// If omitted then the function is disabled.
	Ngrams NgramConf `json:"ngrams"`

	// VerticalFile can be either a path to a single file
	// or a path to a directory containing multiple vertical
	// files (then we assume all the vertical files are of the
	// same structure)
	VerticalFile string `json:"verticalFile"`

	DBFile         string       `json:"dbFile"`
	DBConfSettings []string     `json:"dbConfSettings"`
	Encoding       string       `json:"encoding"`
	SelfJoin       SelfJoinConf `json:"selfJoin"`
	IndexedCols    []string     `json:"indexedCols"`
	BibView        BibViewConf  `json:"bibView"`

	Filter FilterConf `json:"filter"`
}

func (c *VTEConf) UsesSelfJoin() bool {
	return c.SelfJoin.GeneratorFn != ""
}

func (c *VTEConf) HasConfiguredBib() bool {
	return c.BibView.IDAttr != "" && len(c.BibView.Cols) > 0
}

func (c *VTEConf) GetCorpus() string {
	return c.Corpus
}

func (c *VTEConf) GetAtomStructure() string {
	return c.AtomStructure
}

func (c *VTEConf) GetAtomParentStructure() string {
	return c.AtomParentStructure
}

func (c *VTEConf) GetStackStructEval() bool {
	return c.StackStructEval
}

func (c *VTEConf) GetStructures() map[string][]string {
	return c.Structures
}

func (c *VTEConf) GetNgrams() *NgramConf {
	return &c.Ngrams
}

func (c *VTEConf) HasConfiguredFilter() bool {
	return c.Filter.Lib != "" && c.Filter.Fn != ""
}

func (c *VTEConf) GetFilterLib() string {
	return c.Filter.Lib
}

func (c *VTEConf) GetFilterFn() string {
	return c.Filter.Fn
}

func (c *VTEConf) GetMaxNumErrors() int {
	return c.MaxNumErrors
}

func (c *VTEConf) GetDbConfSettings() []string {
	return c.DBConfSettings
}

func LoadConf(confPath string) (*VTEConf, error) {
	rawData, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, err
	}
	var conf VTEConf
	err2 := json.Unmarshal(rawData, &conf)
	if err2 != nil {
		return nil, err2
	}
	return &conf, nil
}
