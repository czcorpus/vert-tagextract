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

	"github.com/czcorpus/vert-tagextract/v3/db"
	"github.com/rs/zerolog/log"
)

const (
	passwordReplacement = "*****"
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
	NgramSize int  `json:"ngramSize"`
	CalcARF   bool `json:"calcARF"`

	// VertColumns specifies which columns should be extracted from
	// a vertical file along with a modifier function (e.g. for converting
	// values to lowercase) and a "role" tag (specifying whether the column
	// is "word", "lemma", "sublemma", "tag", "pos")
	VertColumns db.VertColumns `json:"vertColumns"`

	// Legacy values

	// AttrColumns
	//
	// Deprecated: please use VertColumns instead which groups idx and mod function
	AttrColumns []int `json:"attrColumns,omitempty"`

	// ColumnMods
	//
	// Deprecated: please use VertColumns instead which groups idx and mod function
	ColumnMods []string `json:"columnMods,omitempty"`
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

// IsZero returns true if the object contains all the attributes set to their
// respective zero values (CalcARF == 0, len(VertColumns) == 0 etc.)
// This is used e.g. to reset n-gram configuration in CNC-MASM
func (nc *NgramConf) IsZero() bool {
	return !nc.CalcARF && len(nc.VertColumns) == 0 && len(nc.ColumnMods) == 0 &&
		len(nc.AttrColumns) == 0 && nc.NgramSize == 0
}

// --------------------------

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

	// RemoveEntriesBeforeDate allows for a "moving window" data
	// processing where we regularly remove old records and add some
	// new ones. This value specifies which oldest date should
	// be preserved. Please note that this also requires setting
	// the DateAttr
	RemoveEntriesBeforeDate *string `json:"removeEntriesBeforeDate"`

	// DateAttr is used along with RemoveEntriesBeforeDate
	// so vert-tagextract knows by which attribute to filter the values.
	DateAttr *db.DateAttr `json:"dateAttr"`

	DB db.Conf `json:"db"`

	Encoding    string          `json:"encoding"`
	SelfJoin    db.SelfJoinConf `json:"selfJoin"`
	IndexedCols []string        `json:"indexedCols"`
	BibView     db.BibViewConf  `json:"bibView"`

	Filter FilterConf `json:"filter"`

	Verbosity int `json:"verbosity"`
}

func (c *VTEConf) DefinesMovingDataWindow() bool {
	return c.RemoveEntriesBeforeDate != nil && *c.RemoveEntriesBeforeDate != ""
}

func (c *VTEConf) Validate() error {
	if c.VerticalFile != "" && len(c.VerticalFiles) > 0 {
		return fmt.Errorf("cannot use verticalFile and verticalFiles at the same time")
	}
	if c.RemoveEntriesBeforeDate != nil && c.DateAttr == nil {
		return fmt.Errorf("moving data window defined via *removeEntriesBeforeDate*, but no *dateAttr* found")
	}
	return nil
}

func (c *VTEConf) HasConfiguredFilter() bool {
	return c.Filter.Lib != "" && c.Filter.Fn != ""
}

func (c *VTEConf) HasConfiguredVertical() bool {
	return c.VerticalFile != "" || len(c.VerticalFiles) > 0
}

func (c *VTEConf) GetDefinedVerticals() []string {
	if c.VerticalFile != "" {
		return []string{c.VerticalFile}
	}
	return c.VerticalFiles
}

// WithoutPassword returns a new semi-shallow copy of the called
// config with sensitive information replaced by `*`. By the
// "semi-shallownes" we mean that in case a sensitive information
// overwriting would affect the original object, such part will
// be provided as a deep copy.
func (c *VTEConf) WithoutPasswords() VTEConf {
	ans := *c
	if ans.DB.Password != "" {
		ans.DB.Password = passwordReplacement
	}
	return ans
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
