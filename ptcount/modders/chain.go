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
	"github.com/rs/zerolog/log"
)

const (
	TransformerToLower   = "toLower"
	TransformerIdentity  = "identity"
	TransformerFirstChar = "firstChar"
)

// StringTransformer represents a type which is able
// to modify a string (e.g. to take a substring)
type StringTransformer interface {
	Transform(s string) string
}

type StringTransformerChain struct {
	fn []StringTransformer
}

func NewStringTransformerChain(fn []StringTransformer) *StringTransformerChain {
	return &StringTransformerChain{fn: fn}
}

func (m *StringTransformerChain) Mod(s string) string {
	ans := s
	for _, mod := range m.fn {
		ans = mod.Transform(ans)
	}
	return ans
}

func StringTransformerFactory(name string) StringTransformer {
	switch name {
	case TransformerToLower:

		return ToLower{}
	case TransformerFirstChar:
		return FirstChar{}

	case "", TransformerIdentity:
		return Identity{}
	}
	log.Printf("WARNING: unknown modder function %s", name)
	return nil
}
