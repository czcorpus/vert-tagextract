// Copyright 2019 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2019 Charles University, Faculty of Arts,
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
package ptcount

import (
	"strings"

	"github.com/tomachalek/vertigo/v2"
)

// ColumnCounter stores token tuple along with abs. freq.
// information
type ColumnCounter struct {
	count  int
	values []string
	arf    *WordARF // can be nil
}

// Width says how many columns are used for
// unique records in the result
// (e.g. [word, lemma, pos] means width of 3)
func (c *ColumnCounter) Width() int {
	return len(c.values)
}

// HasARF tests whether ARF calculation
// storage is present. If it is not then
// it means either the job configuration
// does not want ARF to be calculated of
// that it is not set for the specific
// record yet.
func (c *ColumnCounter) HasARF() bool {
	return c.arf != nil
}

// AddARF creates a new helper record to
// calculate ARF for the record.
func (c *ColumnCounter) AddARF(tk *vertigo.Token) {
	c.arf = &WordARF{
		ARF:        0,
		PrevTokIdx: -1,
		FirstIdx:   tk.Idx,
	}
}

// ARF returns ARF helper record
func (c *ColumnCounter) ARF() *WordARF {
	return c.arf
}

// MapTuple calls the provided function on all
// of stored columns from vertical file
// (e.g. fn([word]) then fn([lemma]) then fn([pos]))
func (c *ColumnCounter) MapTuple(fn func(item string, i int)) {
	for i, v := range c.values {
		fn(v, i)
	}
}

// Count tells how many occurences of the
// tuple has been found.
func (c *ColumnCounter) Count() int {
	return c.count
}

// IncCount increase number of occurences for the token tuple
func (c *ColumnCounter) IncCount() {
	c.count++
}

// NewColumnCounter creates a new token tuple with count = 1
func NewColumnCounter(values []string) *ColumnCounter {
	return &ColumnCounter{
		count:  1,
		values: values,
	}
}

// MkTupleKey creates a string key out of provided list of column values.
// This is used internally to countn
func MkTupleKey(values []string) string {
	return strings.Join(values, "")
}
