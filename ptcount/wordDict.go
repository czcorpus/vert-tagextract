// Copyright 2020 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2020 Charles University, Faculty of Arts,
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

// WordDict is basically a bidirectional map for mapping
// between words and ints and ints and words. It is used to
// reduce memory usage when collecting n-grams.
type WordDict struct {
	counter int
	data    map[string]int
	dataRev map[int]string
}

// Add adds a word to the dictionary and returns
// its numeric representation.
func (w *WordDict) Add(word string) int {
	v, ok := w.data[word]
	if !ok {
		w.counter++
		w.data[word] = w.counter
		w.dataRev[w.counter] = word
		return w.counter

	} else {
		return v
	}
}

// Get returns a word based on its integer representation.
func (w *WordDict) Get(idx int) string {
	return w.dataRev[idx]
}

func (w *WordDict) Size() int {
	return len(w.data)
}

func NewWordDict() *WordDict {
	return &WordDict{
		data:    make(map[string]int),
		dataRev: make(map[int]string),
	}
}
