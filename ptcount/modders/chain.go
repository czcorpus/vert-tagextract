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

package modders

import (
	"log"
)

// Modder represents a type which is able
// to modify a string (e.g. to take a substring)
type Modder interface {
	Mod(s string) string
}

type ModderChain struct {
	fn []Modder
}

func NewModderChain(fn []Modder) *ModderChain {
	return &ModderChain{fn: fn}
}

func (m *ModderChain) Mod(s string) string {
	ans := s
	for _, mod := range m.fn {
		ans = mod.Mod(ans)
	}
	return ans
}

func ModderFactory(name string) Modder {
	if name == "toLower" {
		return ToLower{}

	} else if name == "firstChar" {
		return FirstChar{}

	} else if name == "" {
		return Identity{}
	}
	log.Printf("WARNING: unknown modder function %s", name)
	return nil
}
