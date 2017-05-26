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

package vteconf

import (
	"encoding/json"
	"io/ioutil"
)

type SelfJoinConf struct {
	ArgColumns  []string `json:"argColumns"`
	GeneratorFn string   `json:"generatorFn"`
}

type BibViewConf struct {
	Cols   []string `json:"cols"`
	IDAttr string   `json:idAttr"`
}

type VTEConf struct {
	Corpus        string              `json:"corpus"`
	VerticalFile  string              `json:"verticalFile"`
	DBFile        string              `json:"dbFile"`
	Encoding      string              `json:"encoding"`
	AtomStructure string              `json:"atomStructure"`
	SelfJoin      SelfJoinConf        `json:"selfJoin"`
	Structures    map[string][]string `json:"structures"`
	IndexedCols   []string            `json:"indexedCols"`
	BibView       BibViewConf         `json:"bibView"`
}

func (c *VTEConf) UsesSelfJoin() bool {
	return c.SelfJoin.GeneratorFn != ""
}

func (c *VTEConf) HasConfiguredBib() bool {
	return c.BibView.IDAttr != "" && len(c.BibView.Cols) > 0
}

func LoadConf(confPath string) *VTEConf {
	rawData, err := ioutil.ReadFile(confPath)
	if err != nil {
		panic(err)
	}
	var conf VTEConf
	err2 := json.Unmarshal(rawData, &conf)
	if err2 != nil {
		panic(err2)
	}
	return &conf
}
