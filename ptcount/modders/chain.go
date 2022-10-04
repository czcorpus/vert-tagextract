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
	"strings"

	"github.com/rs/zerolog/log"
)

const (
	TransformerToLower       = "toLower"
	TransformerIdentity      = "identity"
	TransformerFirstChar     = "firstChar"
	TransformerPosPenn       = "penn"
	TransformerPosCSCNC2020  = "cs_cnc2020"
	TransformerPosCSCNC2000  = "cs_cnc2000"
	TransformerPosCNC2000Spk = "cs_cnc2000_spk"
)

// StringTransformer represents a type which is able
// to modify a string (e.g. to take a substring)
type StringTransformer interface {
	Transform(s string) string
}

type StringTransformerChain struct {
	fn []StringTransformer
}

func NewStringTransformerChain(specif string) *StringTransformerChain {
	values := strings.Split(specif, ":")
	if len(values) > 0 {
		mod := make([]StringTransformer, 0, len(values))
		for _, v := range values {
			mod = append(mod, StringTransformerFactory(v))
		}
		return &StringTransformerChain{mod}
	}
	return &StringTransformerChain{fn: []StringTransformer{}}
}

func (m *StringTransformerChain) Transform(s string) string {
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
	case TransformerFirstChar,
		TransformerPosCSCNC2020,
		TransformerPosCSCNC2000,
		TransformerPosCNC2000Spk:
		return FirstChar{}
	case TransformerPosPenn:
		return Penn2Pos{}
	case "", TransformerIdentity:
		return Identity{}
	}
	log.Printf("WARNING: unknown modder function %s", name)
	return nil
}
