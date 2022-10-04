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

import "strings"

var (
	pennTags = map[string]string{
		"CC":   "J", //  Coordinating conjunction
		"CD":   "C", //  Cardinal number
		"DT":   "X", //  Determiner
		"EX":   "X", //  Existential there
		"FW":   "X", //  Foreign word
		"IN":   "R", //  Preposition or subordinating conjunction
		"JJ":   "A", //  Adjective
		"JJR":  "A", // Adjective, comparative
		"JJS":  "A", // Adjective, superlative
		"LS":   "X", //  List item marker
		"MD":   "X", //  Modal
		"NN":   "N", //  Noun, singular or mass
		"NNS":  "N", // Noun, plural
		"NNP":  "X", // Proper noun, singular
		"NNPS": "X", //    Proper noun, plural
		"PDT":  "X", // Predeterminer
		"POS":  "X", // Possessive ending
		"PRP":  "P", // Personal pronoun
		"PRP$": "P", //    Possessive pronoun
		"RB":   "D", //  Adverb
		"RBR":  "D", // Adverb, comparative
		"RBS":  "D", // Adverb, superlative
		"RP":   "T", //  Particle
		"SYM":  "X", // Symbol
		"TO":   "X", //  to
		"UH":   "I", //  Interjection
		"VB":   "V", //  Verb, base form
		"VBD":  "V", // Verb, past tense
		"VBG":  "V", // Verb, gerund or present participle
		"VBN":  "V", // Verb, past participle
		"VBP":  "V", // Verb, non-3rd person singular present
		"VBZ":  "V", // Verb, 3rd person singular present
		"WDT":  "V", // Wh-determiner
		"WP":   "P", //  Wh-pronoun
		"WP$":  "P", // Possessive wh-pronoun
		"WRB":  "D", // Wh-adverb
	}
)

type ToLower struct{}

func (m ToLower) Transform(s string) string {
	return strings.ToLower(s)
}

type FirstChar struct{}

func (m FirstChar) Transform(s string) string {
	return s[:1]
}

type Identity struct{}

func (m Identity) Transform(s string) string {
	return s
}

type Penn2Pos struct{}

func (pp Penn2Pos) Transform(s string) string {
	v, ok := pennTags[s]
	if !ok {
		return "X"
	}
	return v
}
