// Copyright 2026 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2026 Charles University, Faculty of Arts,
//                Department of Linguistics
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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitOtherAttrs(t *testing.T) {
	ltg := &LTUDGen{}

	t.Run("equal-length multivalues produce combinations by position", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{"a1|a2|a3", "b1|b2|b3"})
		assert.Equal(t, [][]string{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", "b3"},
		}, result)
	})

	t.Run("unequal lengths pad shorter value with empty strings", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{"a1|a2|a3", "b1|b2"})
		assert.Equal(t, [][]string{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", ""},
		}, result)
	})

	t.Run("single values without pipe produce one combination", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{"a1", "b1"})
		assert.Equal(t, [][]string{
			{"a1", "b1"},
		}, result)
	})

	t.Run("single attribute with multiple values", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{"a1|a2|a3"})
		assert.Equal(t, [][]string{
			{"a1"},
			{"a2"},
			{"a3"},
		}, result)
	})

	t.Run("empty input returns empty result", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{})
		assert.Empty(t, result)
	})

	t.Run("first value is longest and others are padded", func(t *testing.T) {
		result := ltg.SplitOtherAttrs([]string{"a1|a2|a3", "b1"})
		assert.Equal(t, [][]string{
			{"a1", "b1"},
			{"a2", ""},
			{"a3", ""},
		}, result)
	})
}
