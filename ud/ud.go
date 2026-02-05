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

package ud

import (
	"fmt"
	"sort"
	"strings"
)

type Feat [2]string

func (f Feat) Key() string {
	return f[0]
}

func (f Feat) Value() string {
	return f[1]
}

// -------

type FeatList []Feat

func (f FeatList) Normalize() {
	sort.SliceStable(f, func(i, j int) bool {
		return f[i].Key() < f[j].Key()
	})
}

func (f FeatList) Key() string {
	var ans strings.Builder
	for i, v := range f {
		if i > 0 {
			ans.WriteByte('|')
		}
		ans.WriteString(fmt.Sprintf("%s=%s", v[0], v[1]))
	}
	return ans.String()
}

func ParseFeats(s string) (FeatList, error) {
	items := strings.Split(s, "|")
	feats := make(FeatList, 0, len(items)+1) // +1 is for PoS added by the caller
	for _, item := range items {
		tmp := strings.SplitN(item, "=", 2)
		if len(tmp) == 0 || item == "" {
			return []Feat{}, nil
		}
		if len(tmp) == 1 {
			return []Feat{}, fmt.Errorf("unparseable feature '%s'", item)
		}
		if tmp[0] == "_" {
			continue
		}
		feats = append(feats, Feat{tmp[0], tmp[1]})
	}
	return feats, nil
}
