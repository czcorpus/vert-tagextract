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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tomachalek/vertigo/v3"
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
	stack.begin(0, createDocStructure("foo", "bar"))
	stack.begin(1, createDocStructure("foo", "baz"))
	assert.Equal(t, 2, stack.Size())
}

func TestStackBeginFunction(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(0, st1)
	st2 := createPStructure("1")
	stack.begin(1, st2)
	assert.Equal(t, stack.lastItem.value.elm, st2)
	assert.Equal(t, stack.lastItem.value.lineOpen, 1)
	assert.Equal(t, stack.lastItem.prev.value.elm, st1)
	assert.Equal(t, stack.lastItem.prev.value.lineOpen, 0)
}

func TestStackEndFunction(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(0, st1)
	st2 := createPStructure("1")
	stack.begin(1, st2)

	stack.end(0, "p")
	stack.end(1, "doc")
	assert.Nil(t, stack.lastItem)
}

func TestStackNestingError(t *testing.T) {
	stack := structStack{}
	st1 := createDocStructure("poetry", "1981")
	stack.begin(0, st1)
	st2 := createPStructure("1")
	stack.begin(1, st2)
	_, err := stack.end(3, "doc")
	assert.Error(t, err)
}

func TestStackForEachAttrFn(t *testing.T) {
	stack := structStack{}
	stack.begin(0, createDocStructure("poetry", "1981"))
	stack.begin(1, createPStructure("27"))
	tst := make(map[string]string)
	names := make(map[string]bool)
	stack.ForEachAttr(func(sname string, attr string, val string) bool {
		names[sname] = true
		tst[attr] = val
		return true
	})
	_, ok := names["doc"]
	assert.True(t, ok)
	_, ok = names["p"]
	assert.True(t, ok)
	assert.True(t, tst["category"] == "poetry")
	assert.True(t, tst["year"] == "1981")
	assert.True(t, tst["num"] == "27")
	assert.Equal(t, 3, len(tst))
}

func TestStackForEachEarlyExit(t *testing.T) {
	stack := structStack{}
	stack.begin(0, &vertigo.Structure{
		Name: "doc",
		Attrs: map[string]string{
			"attr1": "val1",
		},
	})
	stack.begin(1, &vertigo.Structure{
		Name: "doc",
		Attrs: map[string]string{
			"attr2": "val2",
		},
	})
	stack.begin(2, &vertigo.Structure{
		Name: "doc",
		Attrs: map[string]string{
			"attr3": "val3",
		},
	})
	tst := make(map[string]string)
	names := make(map[string]bool)
	stack.ForEachAttr(func(sname string, attr string, val string) bool {
		names[sname] = true
		tst[attr] = val
		return !(sname == "doc" && attr == "attr3" && val == "val3")
	})

	_, ok := tst["attr3"]
	assert.True(t, ok)
	_, ok = tst["attr2"]
	assert.False(t, ok)
	_, ok = tst["attr1"]
	assert.False(t, ok)
}

func TestNewStructStack(t *testing.T) {
	stack := newStructStack()
	assert.Nil(t, stack.lastItem)
	assert.Equal(t, 0, stack.size)
}

// ----------------------------

func TestDefaultAccumBegin(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*AccumItem)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(0, st1)
	st2 := createPStructure("1")
	accum.begin(1, st2)
	assert.Equal(t, st1, accum.elms[st1.Name].elm)
	assert.Equal(t, 0, accum.elms[st1.Name].lineOpen)
	assert.Equal(t, st2, accum.elms[st2.Name].elm)
	assert.Equal(t, 1, accum.elms[st2.Name].lineOpen)
	assert.Equal(t, 2, len(accum.elms))
}

func TestDefaultAccumEnd(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*AccumItem)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(0, st1)
	st2 := createPStructure("1")
	accum.begin(1, st2)

	accum.end(10, "p")
	accum.end(11, "doc")
	assert.Equal(t, 0, len(accum.elms))
}

func TestDefaultAccumBadNesting(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*AccumItem)
	st1 := createDocStructure("poetry", "1981")
	accum.begin(0, st1)
	st2 := createPStructure("1")
	accum.begin(1, st2)
	accum.end(2, "doc")
	assert.Equal(t, 1, len(accum.elms))
	assert.Equal(t, st2, accum.elms[st2.Name].elm)
	accum.end(3, "p")
	assert.Equal(t, 0, len(accum.elms))
}

func TestDefaultAccumForEachAttrFn(t *testing.T) {
	accum := defaultAccum{}
	accum.elms = make(map[string]*AccumItem)
	accum.begin(0, createDocStructure("poetry", "1981"))
	accum.begin(1, createPStructure("27"))
	tst := make(map[string]string)
	names := make(map[string]bool)
	accum.ForEachAttr(func(sname string, attr string, val string) bool {
		names[sname] = true
		tst[attr] = val
		return true
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
