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
	"encoding/json"
	"io/ioutil"
	"log"
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

// VTEConf holds configuration for a concrete
// data extraction task.
type VTEConf struct {
	Corpus          string              `json:"corpus"`
	AtomStructure   string              `json:"atomStructure"`
	StackStructEval bool                `json:"stackStructEval"`
	Structures      map[string][]string `json:"structures"`

	// ColumnCounts configures positional attributes (referred by their
	// column position) we want to count. This can be used to extract
	// all the unique PoS tags or frequency information about words/lemmas.
	// If omitted then the function is disabled.
	CountColumns []int `json:"countColumns"`

	VerticalFile string       `json:"verticalFile"`
	DBFile       string       `json:"dbFile"`
	Encoding     string       `json:"encoding"`
	SelfJoin     SelfJoinConf `json:"selfJoin"`
	IndexedCols  []string     `json:"indexedCols"`
	BibView      BibViewConf  `json:"bibView"`
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

func (c *VTEConf) GetStackStructEval() bool {
	return c.StackStructEval
}

func (c *VTEConf) GetStructures() map[string][]string {
	return c.Structures
}

func (c *VTEConf) GetCountColumns() []int {
	return c.CountColumns
}

func LoadConf(confPath string) *VTEConf {
	rawData, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal(err)
	}
	var conf VTEConf
	err2 := json.Unmarshal(rawData, &conf)
	if err2 != nil {
		log.Fatal(err2)
	}
	return &conf
}
