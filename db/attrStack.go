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

package db

import (
	"fmt"

	"github.com/tomachalek/vertigo"
)

// -----------------------------------------------

type attrAccumulator interface {
	begin(v *vertigo.Structure)
	end(name string) *vertigo.Structure
	forEachAttr(fn func(structure string, attr string, val string))
}

// -----------------------------------------------

type stackItem struct {
	prev  *stackItem
	value *vertigo.Structure
}

type structStack struct {
	lastItem *stackItem
	size     int
}

func (s *structStack) begin(item *vertigo.Structure) {
	tmp := s.lastItem
	s.lastItem = &stackItem{prev: tmp, value: item}
	s.size++
}

func (s *structStack) end(name string) *vertigo.Structure {
	if s.lastItem.value.Name != name {
		panic(fmt.Sprintf("Stack error. Expected: %s, got: %s", s.lastItem.value.Name, name))
	}
	tmp := s.lastItem
	s.lastItem = s.lastItem.prev
	s.size--
	return tmp.value
}

func (s *structStack) Size() int {
	return s.size
}

func (s *structStack) forEachAttr(fn func(structure string, attr string, val string)) {
	st := s.lastItem
	for st != nil {
		for k, v := range st.value.Attrs {
			fn(st.value.Name, k, v)
		}
		st = st.prev
	}
}

func newStructStack() *structStack {
	return &structStack{}
}

// -----------------------------------------------

// defaultAccum is a structure accumulator which
// does not care about xml-like nesting. But there
// is a limitation in the sense that one cannot
// nest a single structure to itself
// (e.g.: <p>...<p>...</p>..</p>).
type defaultAccum struct {
	elms map[string]*vertigo.Structure
}

func (sa *defaultAccum) begin(v *vertigo.Structure) {
	sa.elms[v.Name] = v
}

func (sa *defaultAccum) end(name string) *vertigo.Structure {
	tmp := sa.elms[name]
	delete(sa.elms, name)
	return tmp
}

func (sa *defaultAccum) forEachAttr(fn func(structure string, attr string, val string)) {
	for name, structItem := range sa.elms {
		for attr, val := range structItem.Attrs {
			fn(name, attr, val)
		}
	}
}

func newDefaultAccum() *defaultAccum {
	return &defaultAccum{elms: make(map[string]*vertigo.Structure)}
}
