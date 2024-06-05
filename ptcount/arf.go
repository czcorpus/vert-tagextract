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

package ptcount

import (
	"fmt"
	"math"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v2/cnf"
	"github.com/czcorpus/vert-tagextract/v2/ptcount/modders"

	"github.com/tomachalek/vertigo/v5"
)

// Calculate ARF for processed n-grams. Please note that the way
// this module closes unfinished n-grams is not 100% compatible with
// the one TTExtractor does this. The difference may occur in case
// a corpus contains nested atom structures (e.g. <p> within <p>,
// <s> within <s> etc.).

// For more information about ARF definition and possible calculation
// please see e.g.:
// https://www.sketchengine.eu/documentation/average-reduced-frequency/
// http://wiki.korpus.cz/doku.php/en:pojmy:arf

// min is a single-purpose min function
// where we compare float (= avg. distance)
// with int (= actual distance)
func min(v1 float64, v2 int) float64 {
	if v1 < float64(v2) {
		return v1
	}
	return float64(v2)
}

// WordARF is used as an attribute of NgramCounter
// to calculate ARF. The attributes are designed for
// two-pass calculation where in the 1st pass we obtain
// avg distance between word instance and in the 2nd
// pass we actually calculate the result. This method
// is slower (we parse the vertical file two times)
// but it needs less memory compared with single pass
// method.
type WordARF struct {
	ARF        float64
	FirstIdx   int
	PrevTokIdx int
}

func (ws WordARF) String() string {
	return fmt.Sprintf("WordARF: {arf: %01.2f, 1st: %d, last: %d", ws.ARF, ws.FirstIdx, ws.PrevTokIdx)
}

// ARFCalculator calculates ARF for all the
// [ngram_uniq_id] => NgramCounter pairs we
// obtain in the 1st pass.
type ARFCalculator struct {
	ngramConf     *cnf.NgramConf
	counts        map[string]*NgramCounter
	currSentence  [][]int
	numTokens     int
	columnModders []*modders.StringTransformerChain
	wordDict      *WordDict
	atomStruct    string
}

// NewARFCalculator is the recommended factory to create an instance of the type
func NewARFCalculator(counts map[string]*NgramCounter, ngramConf *cnf.NgramConf, numTokens int,
	columnModders []*modders.StringTransformerChain, wordDict *WordDict, atomStruct string) *ARFCalculator {
	return &ARFCalculator{
		numTokens:     numTokens,
		counts:        counts,
		currSentence:  make([][]int, 0, 20),
		ngramConf:     ngramConf,
		columnModders: columnModders,
		atomStruct:    atomStruct,
		wordDict:      wordDict,
	}
}

// ProcToken is called by vertigo parser when a token is encountered
func (arfc *ARFCalculator) ProcToken(tk *vertigo.Token, line int, err error) error {
	attributes := make([]int, arfc.ngramConf.VertColumns.MaxColumn()+1)
	for _, vertCol := range arfc.ngramConf.VertColumns {
		v := tk.PosAttrByIndex(vertCol.Idx)
		attributes[vertCol.Idx] = arfc.wordDict.Add(arfc.columnModders[vertCol.Idx].Transform(v))
	}

	arfc.currSentence = append(arfc.currSentence, attributes)
	if len(arfc.currSentence) >= arfc.ngramConf.NgramSize {
		ngram := NewNgramCounter(arfc.ngramConf.NgramSize)
		startPos := len(arfc.currSentence) - arfc.ngramConf.NgramSize
		for i := startPos; i < len(arfc.currSentence); i++ {
			ngram.AddToken(arfc.currSentence[i])
		}
		key := ngram.UniqueID()
		cnt, ok := arfc.counts[key]
		if !ok {
			log.Warn().Str("token", key).Msg("token not found in previously processed data")
			return nil
		}
		if !cnt.HasARF() {
			cnt.AddARF(tk)
		}
		if cnt.ARF().PrevTokIdx > -1 {
			cnt.ARF().ARF += min(float64(arfc.numTokens)/float64(cnt.Count()), tk.Idx-cnt.ARF().PrevTokIdx)
		}
		cnt.ARF().PrevTokIdx = tk.Idx
	}
	return nil
}

// ProcStruct is used by Vertigo parser but we don't need it here
func (arfc *ARFCalculator) ProcStruct(strc *vertigo.Structure, line int, err error) error {
	return err
}

// ProcStructClose is used by Vertigo parser but we don't need it here
func (arfc *ARFCalculator) ProcStructClose(strc *vertigo.StructureClose, line int, err error) error {
	if strc.Name == arfc.atomStruct {
		arfc.currSentence = arfc.currSentence[:0]
	}
	return err
}

// Finalize performs some final calculations on obtained
// (and continuouslz calculated) data. It is required to
// to obtain correct ARF results.
func (arfc *ARFCalculator) Finalize() {
	for k, val := range arfc.counts {
		if val.HasARF() {
			cnt := arfc.counts[k]
			avgDist := float64(arfc.numTokens) / float64(cnt.Count())
			val.ARF().ARF += min(avgDist, val.ARF().FirstIdx+arfc.numTokens-val.ARF().PrevTokIdx)
			val.ARF().ARF = math.Round(val.ARF().ARF/avgDist*1000) / 1000.0
		}
	}
}
