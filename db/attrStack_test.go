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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tomachalek/vertigo"
)

func createDocStructure(category string, year string) *vertigo.Structure {
	attrs := make(map[string]string)
	attrs["category"] = category
	attrs["year"] = year
	return &vertigo.Structure{
		Name:  "doc",
		Attrs: attrs,
	}
}

func createPStructure(num string) *vertigo.Structure {
	attrs := make(map[string]string)
	attrs["num"] = num
	return &vertigo.Structure{
		Name:  "p",
		Attrs: attrs,
	}
}

func TestSizeEmpty(t *testing.T) {
	stack := structStack{}
	assert.Equal(t, 0, stack.Size())
}

func TestSizeNonEmpty(t *testing.T) {
	stack := structStack{}
	stack.begin(createDocStructure("foo", "bar"))
	stack.begin(createDocStructure("foo", "baz"))
	assert.Equal(t, 2, stack.Size())
}

func TestStackBeginFunction(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(st1)
	st2 := createPStructure("1")
	stack.begin(st2)
	assert.Equal(t, stack.lastItem.value, st2)
	assert.Equal(t, stack.lastItem.prev.value, st1)
}

func TestStackEndFunction(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(st1)
	st2 := createPStructure("1")
	stack.begin(st2)

	stack.end("p")
	stack.end("doc")
	assert.Nil(t, stack.lastItem)
}

func TestStackNestingError(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(st1)
	st2 := createPStructure("1")
	stack.begin(st2)

	assert.Panics(t, func() {
		stack.end("doc")
		stack.end("p")
	})
}

func TestStackForEachAttrFn(t *testing.T) {
	stack := structStack{}
	stack.begin(createDocStructure("poetry", "1981"))
	stack.begin(createPStructure("27"))
	tst := make(map[string]string)
	names := make(map[string]bool)
	stack.forEachAttr(func(sname string, attr string, val string) {
		names[sname] = true
		tst[attr] = val
	})
	_, ok := names["doc"]
	assert.True(t, ok == true)
	_, ok = names["p"]
	assert.True(t, ok == true)
	assert.True(t, tst["category"] == "poetry")
	assert.True(t, tst["year"] == "1981")
	assert.True(t, tst["num"] == "27")
	assert.Equal(t, 3, len(tst))
}

func TestNewStructStack(t *testing.T) {
	stack := newStructStack()
	assert.Nil(t, stack.lastItem)
	assert.Equal(t, 0, stack.size)
}

// ----------------------------

func TestDefaultAccumBegin(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*vertigo.Structure)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(st1)
	st2 := createPStructure("1")
	accum.begin(st2)
	assert.Equal(t, st1, accum.elms[st1.Name])
	assert.Equal(t, st2, accum.elms[st2.Name])
	assert.Equal(t, 2, len(accum.elms))
}

func TestDefaultAccumEnd(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*vertigo.Structure)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(st1)
	st2 := createPStructure("1")
	accum.begin(st2)

	accum.end("p")
	accum.end("doc")
	assert.Equal(t, 0, len(accum.elms))
}

func TestDefaultAccumBadNesting(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*vertigo.Structure)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(st1)
	st2 := createPStructure("1")
	accum.begin(st2)
	accum.end("doc")
	assert.Equal(t, 1, len(accum.elms))
	assert.Equal(t, st2, accum.elms[st2.Name])
	accum.end("p")
	assert.Equal(t, 0, len(accum.elms))
}

func TestDefaultAccumForEachAttrFn(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*vertigo.Structure)
	accum.begin(createDocStructure("poetry", "1981"))
	accum.begin(createPStructure("27"))
	tst := make(map[string]string)
	names := make(map[string]bool)
	accum.forEachAttr(func(sname string, attr string, val string) {
		names[sname] = true
		tst[attr] = val
	})
	_, ok := names["doc"]
	assert.True(t, ok == true)
	_, ok = names["p"]
	assert.True(t, ok == true)
	assert.True(t, tst["category"] == "poetry")
	assert.True(t, tst["year"] == "1981")
	assert.True(t, tst["num"] == "27")
	assert.Equal(t, 3, len(tst))
}

func TestNewDefaultAccum(t *testing.T) {
	accum := newDefaultAccum()
	assert.NotNil(t, accum.elms)
}
