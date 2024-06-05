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

package cnf

import (
	"testing"

	"github.com/czcorpus/vert-tagextract/v2/db"
	"github.com/stretchr/testify/assert"
)

func TestNgramIsZero(t *testing.T) {
	var cnf NgramConf
	assert.True(t, cnf.IsZero())

	cnf.NgramSize = 1
	assert.False(t, cnf.IsZero())

	cnf.NgramSize = 0
	cnf.CalcARF = true
	assert.False(t, cnf.IsZero())

	cnf.CalcARF = false
	cnf.AttrColumns = []int{0}
	assert.False(t, cnf.IsZero())

	cnf.AttrColumns = nil
	cnf.ColumnMods = []string{"foo"}
	assert.False(t, cnf.IsZero())

	cnf.ColumnMods = nil
	cnf.VertColumns = []db.VertColumn{{Idx: 1}}
	assert.False(t, cnf.IsZero())
}

func TestNgramMaxRequiredColumn(t *testing.T) {
	cnf := NgramConf{
		VertColumns: []db.VertColumn{
			{Idx: 1}, {Idx: 4}, {Idx: 5}, {Idx: 3}, {Idx: 0},
		},
	}
	assert.Equal(t, 5, cnf.MaxRequiredColumn())
}

func TestNgramMaxRequiredColumnNoColumns(t *testing.T) {
	var cnf NgramConf
	assert.Equal(t, 0, cnf.MaxRequiredColumn())
}
