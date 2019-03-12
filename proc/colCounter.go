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
package proc

import (
	"strings"

	"github.com/tomachalek/vertigo"
)

// ColumnCounter stores token tuple along with abs. freq.
// information
type ColumnCounter struct {
	count  int
	values []string
}

// IncCount increase number of occurences for the token tuple
func (c *ColumnCounter) IncCount() {
	c.count++
}

// newColumnCounter creates a new token tuple with count = 1
func newColumnCounter(token *vertigo.Token, countColumns []int) *ColumnCounter {
	values := make([]string, len(countColumns))
	for i, v := range countColumns {
		if v == 0 {
			values[i] = strings.ToLower(token.Word)

		} else {
			values[i] = strings.ToLower(token.Attrs[v-1])
		}
	}
	return &ColumnCounter{
		count:  1,
		values: values,
	}
}

// mkTupleKey creates a string key out of provided list of column values.
// This is used internally to countn
func mkTupleKey(token *vertigo.Token, countColumns []int) string {
	ans := make([]string, len(countColumns))
	for i, v := range countColumns {
		if v == 0 {
			ans[i] = strings.ToLower(token.Word)

		} else {
			ans[i] = strings.ToLower(token.Attrs[v-1])
		}
	}
	return strings.Join(ans, "")
}
