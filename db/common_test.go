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
)

func TestDateAttrValidate(t *testing.T) {
	a := DateAttr("a_b")
	assert.True(t, a.Validate())
	a = DateAttr("doc.real_author")
	assert.True(t, a.Validate())
	a = DateAttr("a.b")
	assert.True(t, a.Validate())
	a = DateAttr("structure")
	assert.False(t, a.Validate())
	a = DateAttr("_a_b")
	assert.False(t, a.Validate())
	a = DateAttr("a.")
	assert.False(t, a.Validate())
	a = DateAttr(".a")
	assert.False(t, a.Validate())
	a = DateAttr("structure.attribute.x")
	assert.False(t, a.Validate())

}
