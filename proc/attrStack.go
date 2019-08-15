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
	"fmt"

	"github.com/tomachalek/vertigo/v2"
)

// -----------------------------------------------

// AttrAccumulator specifies an object able to collect
// (as tokens go) current structural attribute information.
// Under the hood you can imagine something like a non-strict,
// generalized stack.
type AttrAccumulator interface {
	begin(v *vertigo.Structure) error
	end(name string) (*vertigo.Structure, error)
	ForEachAttr(fn func(structure string, attr string, val string) bool)
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

func (s *structStack) begin(item *vertigo.Structure) error {
	tmp := s.lastItem
	s.lastItem = &stackItem{prev: tmp, value: item}
	s.size++
	return nil
}

func (s *structStack) end(name string) (*vertigo.Structure, error) {
	if s.lastItem.value.Name != name {
		return nil, fmt.Errorf("Stack-based processing error. Encountered element: [%s], stack top: [%s]", name, s.lastItem.value.Name)
		panic(fmt.Sprintf("Stack error. Expected: %s, got: %s", s.lastItem.value.Name, name))
	}
	tmp := s.lastItem
	s.lastItem = s.lastItem.prev
	s.size--
	return tmp.value, nil
}

func (s *structStack) Size() int {
	return s.size
}

func (s *structStack) ForEachAttr(fn func(structure string, attr string, val string) bool) {
	st := s.lastItem
	for st != nil {
		for k, v := range st.value.Attrs {
			stay := fn(st.value.Name, k, v)
			if !stay {
				return
			}
		}
		st = st.prev
	}
}

func newStructStack() *structStack {
	return &structStack{}
}

func getElementHintRepr(v *vertigo.Structure) (ident string) {
	identAttrs := []string{"id", "name", "ident", "inst"}
	for _, a := range identAttrs {
		var ok bool
		ident, ok = v.Attrs[a]
		if ok {
			ident = fmt.Sprintf("<%s %s=\"%s\">", v.Name, a, ident)
			break
		}
	}
	if len(ident) == 0 {
		ident = fmt.Sprintf("<%s>", v.Name)
	}
	return
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

func (sa *defaultAccum) begin(v *vertigo.Structure) error {
	prev, ok := sa.elms[v.Name]
	if ok {
		return fmt.Errorf("Self-recursion not allowed, element %s in %s", getElementHintRepr(v), getElementHintRepr(prev))
	}
	sa.elms[v.Name] = v
	return nil
}

func (sa *defaultAccum) end(name string) (*vertigo.Structure, error) {
	tmp, ok := sa.elms[name]
	if ok {
		delete(sa.elms, name)
		return tmp, nil
	}
	return nil, fmt.Errorf("Cannot close element [%s] - opening not found")
}

func (sa *defaultAccum) ForEachAttr(fn func(structure string, attr string, val string) bool) {
	for name, structItem := range sa.elms {
		for attr, val := range structItem.Attrs {
			stay := fn(name, attr, val)
			if !stay {
				return
			}
		}
	}
}

func newDefaultAccum() *defaultAccum {
	return &defaultAccum{elms: make(map[string]*vertigo.Structure)}
}
