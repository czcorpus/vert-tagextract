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

	"github.com/tomachalek/vertigo/v3"
)

// Position specifies positional attributes
// (e.g. word, lemma, tag) at some n-gram position
type Position struct {
	Columns []string
}

// NgramCounter stores an n-gram with multiple attributes
// per position along absolute freq. information and optionally
// with ARF information.
// Please note that it is expected that
// any instance should have at least the first position
// of the n-gram filled-in. That is why it is recommended
// to use the NewNgramCounter() factory which ensures this.
type NgramCounter struct {
	count  int
	tokens []Position
	arf    *WordARF // can be nil
}

func (c *NgramCounter) String() string {
	ans := make([]string, len(c.tokens))
	for i, v := range c.tokens {
		ans[i] = v.Columns[0]
	}
	return strings.Join(ans, "#")
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
	return len(c.tokens[0].Columns)
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

func (c *NgramCounter) columnNgram(colIdx int) string {
	tmp := make([]string, len(c.tokens))
	for i, v := range c.tokens {
		tmp[i] = v.Columns[colIdx]
	}
	return strings.Join(tmp, " ")
}

// ForEachAttr calls the provided function on all
// of stored columns from vertical file
// (e.g. fn([word]) then fn([lemma]) then fn([pos]))
func (c *NgramCounter) ForEachAttr(fn func(item string, i int)) {
	if len(c.tokens) == 1 {
		for i, v := range c.tokens[0].Columns {
			fn(v, i)
		}

	} else {
		for i := range c.tokens[0].Columns {
			fn(c.columnNgram(i), i)
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
func (c *NgramCounter) AddToken(pos []string) {
	c.tokens = append(c.tokens, Position{Columns: pos})
}

// UniqueID creates an unique ngram identifier
func (c *NgramCounter) UniqueID() string {
	ans := make([]string, len(c.tokens))
	for i, pos := range c.tokens {
		ans[i] = strings.Join(pos.Columns, "")
	}
	return strings.Join(ans, " ")
}

// NewNgramCounter creates a new n-mgra with count = 1
func NewNgramCounter(size int, zeroPos []string) *NgramCounter {
	ans := &NgramCounter{
		count:  1,
		tokens: make([]Position, 0, size),
	}
	ans.tokens = append(ans.tokens, Position{Columns: zeroPos})
	return ans
}
