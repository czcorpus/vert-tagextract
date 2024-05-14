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
	"fmt"
	"os"

	"github.com/czcorpus/vert-tagextract/v2/db"
	"github.com/rs/zerolog/log"
)

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
	NgramSize   int            `json:"ngramSize"`
	CalcARF     bool           `json:"calcARF"`
	VertColumns db.VertColumns `json:"vertColumns"`

	// Legacy values

	// AttrColumns
	//
	// Deprecated: please use VertColumns instead which groups idx and mod function
	AttrColumns []int `json:"attrColumns"`

	// ColumnMods
	//
	// Deprecated: please use VertColumns instead which groups idx and mod function
	ColumnMods []string `json:"columnMods"`
}

func (nc *NgramConf) UpgradeLegacy() error {
	if len(nc.AttrColumns) > 0 {
		log.Warn().Msg("upgrading legacy n-gram configuration")
		if len(nc.VertColumns) > 0 && len(nc.VertColumns) != len(nc.AttrColumns) {
			return fmt.Errorf("vertColumns and attrColumns mismatch")
		}
		ans := make(db.VertColumns, len(nc.AttrColumns))
		cmods := nc.ColumnMods
		if len(cmods) == 0 {
			cmods = make([]string, len(nc.AttrColumns))
		}
		for i, v := range nc.AttrColumns {
			ans[i] = db.VertColumn{
				Idx:   v,
				ModFn: cmods[i],
			}
		}
		nc.VertColumns = ans
	}
	return nil
}

func (nc *NgramConf) MaxRequiredColumn() int {
	return nc.VertColumns.MaxColumn()
}

// VTEConf holds configuration for a concrete
// data extraction task.
type VTEConf struct {
	Corpus              string `json:"corpus"`
	ParallelCorpus      string `json:"parallelCorpus,omitempty"`
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
	VerticalFile string `json:"verticalFile,omitempty"`

	// VerticalFiles is an alternative to VerticalFile allowing
	// explicit selection of one or more files to be processed
	// as one.
	VerticalFiles []string `json:"verticalFiles,omitempty"`

	DB db.Conf `json:"db"`

	Encoding    string          `json:"encoding"`
	SelfJoin    db.SelfJoinConf `json:"selfJoin"`
	IndexedCols []string        `json:"indexedCols"`
	BibView     db.BibViewConf  `json:"bibView"`

	Filter FilterConf `json:"filter"`

	Verbosity int `json:"verbosity"`
}

func (c *VTEConf) HasConfiguredFilter() bool {
	return c.Filter.Lib != "" && c.Filter.Fn != ""
}

func LoadConf(confPath string) (*VTEConf, error) {
	rawData, err := os.ReadFile(confPath)
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
