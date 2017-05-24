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
	"github.com/tomachalek/vertigo"
)

type stackItem struct {
	prev  *stackItem
	value *vertigo.Structure
}

type structStack struct {
	lastItem *stackItem
}

func (s *structStack) Push(item *vertigo.Structure) {
	tmp := s.lastItem
	s.lastItem = &stackItem{prev: tmp, value: item}
}

func (s *structStack) Pop() *vertigo.Structure {
	tmp := s.lastItem
	s.lastItem = s.lastItem.prev
	return tmp.value
}

func (s *structStack) GoThroughAttrs(fn func(structure string, attr string, val string)) {
	st := s.lastItem
	for st != nil {
		for k, v := range st.value.Attrs {
			fn(st.value.Name, k, v)
		}
		st = st.prev
	}
}
