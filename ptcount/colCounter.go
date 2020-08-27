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
	"fmt"
	"strings"

	"github.com/tomachalek/vertigo/v4"
)

// Position specifies positional attributes
// (e.g. word, lemma, tag) at some n-gram position
type Position struct {
	Columns []int
}

// NgramCounter stores an n-gram with multiple attributes
// per position along absolute freq. information and optionally
// with ARF information.
type NgramCounter struct {
	count  int
	tokens []Position
	arf    *WordARF // can be nil
}

// Length returns n-gram length (1 = unigram, 2 = bigram,...)
func (c *NgramCounter) Length() int {
	return cap(c.tokens)
}

// CurrLength returns actual n-gram length (i.e. if a trigram has only
// first position filled-in then the returned value is 1)
func (c *NgramCounter) CurrLength() int {
	return len(c.tokens)
}

// Width says how many columns are used for
// unique records in the result
// (e.g. [word, lemma, pos] means width of 3)
func (c *NgramCounter) Width() int {
	if len(c.tokens) > 0 {
		return len(c.tokens[0].Columns)
	}
	return 0
}

// HasARF tests whether ARF calculation
// storage is present. If it is not then
// it means either the job configuration
// does not want ARF to be calculated of
// that it is not set for the specific
// record yet.
func (c *NgramCounter) HasARF() bool {
	return c.arf != nil
}

// AddARF creates a new helper record to
// calculate ARF for the record.
func (c *NgramCounter) AddARF(tk *vertigo.Token) {
	c.arf = &WordARF{
		ARF:        0,
		PrevTokIdx: -1,
		FirstIdx:   tk.Idx,
	}
}

// ARF returns ARF helper record
func (c *NgramCounter) ARF() *WordARF {
	return c.arf
}

func (c *NgramCounter) columnNgram(colIdx int, wd *WordDict) string {
	tmp := make([]string, len(c.tokens))
	for i, v := range c.tokens {
		tmp[i] = wd.Get(v.Columns[colIdx])
	}
	return strings.Join(tmp, " ")
}

func (c *NgramCounter) columnNgramNumeric(colIdx int) string {
	tmp := make([]string, len(c.tokens))
	for i, v := range c.tokens {
		tmp[i] = fmt.Sprint(v.Columns[colIdx])
	}
	return strings.Join(tmp, " ")
}

// ForEachAttr calls the provided function on all
// of stored columns from vertical file
// (e.g. fn([word]) then fn([lemma]) then fn([pos]))
func (c *NgramCounter) ForEachAttr(wDict *WordDict, fn func(item string, i int)) {
	if len(c.tokens) == 1 {
		for i, v := range c.tokens[0].Columns {
			fn(wDict.Get(v), i)
		}

	} else if len(c.tokens) > 1 {
		for i := range c.tokens[0].Columns {
			fn(c.columnNgram(i, wDict), i)
		}
	}
}

// Count tells how many occurences of the
// ngram has been found.
func (c *NgramCounter) Count() int {
	return c.count
}

// IncCount increase number of occurences for the n-gram
func (c *NgramCounter) IncCount() {
	c.count++
}

// AddToken add additional (besides 0th) tokens to the n-gram
func (c *NgramCounter) AddToken(pos []int) {
	c.tokens = append(c.tokens, Position{Columns: pos})
}

// UniqueID creates an unique ngram identifier
func (c *NgramCounter) UniqueID(columns []int) string {
	ans := make([]string, len(columns))
	for i, col := range columns {
		ans[i] = c.columnNgramNumeric(col)
	}
	return strings.Join(ans, " ")
}

// NewNgramCounter creates a new n-gram with count = 1
func NewNgramCounter(size int) *NgramCounter {
	ans := &NgramCounter{
		count:  1,
		tokens: make([]Position, 0, size),
	}
	return ans
}
