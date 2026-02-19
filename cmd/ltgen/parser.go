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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/czcorpus/vert-tagextract/v3/db"
	"github.com/czcorpus/vert-tagextract/v3/livetokens"
	"github.com/czcorpus/vert-tagextract/v3/ud"
	"github.com/rs/zerolog/log"
	"github.com/tomachalek/vertigo/v6"
)

// -------

type ltgConf struct {
	CorpusID     string              `json:"corpusId"`
	Attrs        livetokens.AttrList `json:"attrs"`
	DB           db.Conf             `json:"db"`
	VerticalPath string              `json:"verticalPath"`
}

func LoadConf(path string) (ltgConf, error) {
	var ans ltgConf
	data, err := os.ReadFile(path)
	if err != nil {
		return ans, fmt.Errorf("failed to read LTG conf: %w", err)
	}
	if err := json.Unmarshal(data, &ans); err != nil {
		return ans, fmt.Errorf("failed to read LTG conf: %w", err)
	}
	return ans, nil
}

// -------

type AttrCombination []string

func (ac AttrCombination) Key() string {
	return strings.Join(ac, "|")
}

// -----

type CountedAttrs struct {
	Values   []string
	Feats    ud.FeatList
	Count    int
	LastLine int
}

func (ca CountedAttrs) Key() string {
	return strings.Join(ca.Values, "|") + "|" + ca.Feats.Key()
}

var (
	unparsedFeatsSrch  = regexp.MustCompile(`^[a-zA-Z0-9]+=[a-zA-Z0-9]+(\|[a-zA-Z0-9]+=[a-zA-Z0-9]+)*$`)
	pseudoNumericField = regexp.MustCompile(`([-+]?[0-9]+(/[0-9]*)?`)
)

func (ca CountedAttrs) SeemsValid() bool {
	if slices.ContainsFunc(ca.Values, func(v string) bool {
		return unparsedFeatsSrch.MatchString(v)
	}) {
		return false
	}
	// Now let's check for tuples where
	// only numbers (and possibly whitespaces) are.
	// Such values are possibly wrong
	numEmpty := 0
	for _, v := range ca.Values {
		v = strings.TrimSpace(v)
		if v == "" {
			numEmpty++
			continue
		}
		if pseudoNumericField.MatchString(v) {
			return false
		}
	}
	return numEmpty < len(ca.Values)
}

type LTUDGen struct {
	ctx         context.Context
	attrs       livetokens.AttrList
	corpname    string
	data        map[string]CountedAttrs
	numVertCols int
}

func (ltg *LTUDGen) insertUDFeats(db *sql.Tx, data []ud.FeatList, idRange [2]int64) error {
	log.Debug().Any("idRange", idRange).Int("dataSize", len(data)).Msg("about to insert UD feats")
	currID := idRange[0]
	for _, feats := range data {
		values := make([][]any, 0, len(feats))
		for _, v := range feats {
			values = append(values, []any{v.Key(), v.Value(), currID})
		}
		if err := livetokens.InsertFeats(ltg.ctx, db, ltg.corpname, values); err != nil {
			return fmt.Errorf("failed to insert UD feats for %d: %w", currID, err)
		}
		currID++
	}
	if currID-1 != idRange[1] {
		return fmt.Errorf("id range mismatch: %d-%d vs expected %d-%d", idRange[0], currID-1, idRange[0], idRange[1])
	}
	return nil
}

func (ltg *LTUDGen) StoreToDatabase(db *sql.Tx) error {
	chunkSize := 100
	chunk := make([][]any, chunkSize)
	chunkDependentFeats := make([]ud.FeatList, chunkSize)
	i := 0
	for _, v := range ltg.data {
		if !v.SeemsValid() {
			log.Warn().Strs("values", v.Values).Int("last-line", v.LastLine).Msg("skipping possibly invalid entry")
			continue
		}
		values := make([]any, ltg.attrs.LenWithoutUDFeats()+1) // +1 => `cnt` field
		for i2, v2 := range v.Values {
			values[i2] = v2
		}
		values[len(values)-1] = v.Count
		chunk[i] = values
		chunkDependentFeats[i] = v.Feats
		if i == len(chunk)-1 {
			idRange, err := livetokens.InsertTokens(ltg.ctx, db, ltg.corpname, ltg.attrs, chunk)
			if err != nil {
				return fmt.Errorf("failed to insert: %w", err)
			}
			if err := ltg.insertUDFeats(db, chunkDependentFeats, idRange); err != nil {
				return fmt.Errorf("failed to insert UD feats: %w", err)
			}
			// now reset all
			chunk = make([][]any, chunkSize)
			chunkDependentFeats = make([]ud.FeatList, chunkSize)
			i = 0

		} else {
			i++
		}
	}
	return nil
}

func (ltg *LTUDGen) ProcToken(tk *vertigo.Token, line int, err error) error {
	if ltg.numVertCols != len(tk.Attrs) {
		if ltg.numVertCols == 0 {
			ltg.numVertCols = len(tk.Attrs)

		} else {
			log.Error().
				Int("expectedCols", ltg.numVertCols).
				Int("actualCols", len(tk.Attrs)).
				Int("line", line).
				Msg("reporting invalid vertical line")
		}
	}
	var feats ud.FeatList
	otherAttrs := make([]string, 0, len(ltg.attrs))
	for _, attr := range ltg.attrs {
		val := tk.PosAttrByIndex(attr.VertIdx)
		if attr.IsUDFeats {
			attrs, err := ud.ParseFeats(val)
			if err != nil {
				log.Warn().Err(err).Int("line", line).Msg("failed to parse UD feats, skipping")
				continue
			}
			feats = attrs

		} else {
			otherAttrs = append(otherAttrs, val)
		}
	}
	feats.Normalize()
	newItem := CountedAttrs{
		Values:   otherAttrs,
		Feats:    feats,
		Count:    1,
		LastLine: line,
	}
	niKey := newItem.Key()

	stored, ok := ltg.data[niKey]
	if !ok {
		ltg.data[niKey] = newItem

	} else {
		stored.Count++
		stored.LastLine = line
		ltg.data[niKey] = stored
	}

	return nil
}

func (ltg *LTUDGen) ProcStruct(st *vertigo.Structure, line int, err error) error {
	select {
	case s := <-ltg.ctx.Done():
		return fmt.Errorf("received stop signal: %s", s)
	default:
	}

	return nil
}

func (ltg *LTUDGen) ProcStructClose(st *vertigo.StructureClose, line int, err error) error {
	return nil
}

func ParseFileUD(ctx context.Context, conf ltgConf, db *sql.DB) error {
	parserConf := &vertigo.ParserConf{
		StructAttrAccumulator: "nil",
		Encoding:              "utf-8",
		LogProgressEachNth:    250000, // TODO configurable
		InputFilePath:         conf.VerticalPath,
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to run ltgen+: %w", err)
	}

	proc := &LTUDGen{
		corpname: conf.CorpusID,
		ctx:      ctx,
		attrs:    conf.Attrs,
		data:     make(map[string]CountedAttrs),
	}
	log.Info().Msg("using zero-based indexing when reporting line errors")

	if err := vertigo.ParseVerticalFile(ctx, parserConf, proc); err != nil {
		return fmt.Errorf("failed to run ltgen: %w", err)
	}

	if err := proc.StoreToDatabase(tx); err != nil {
		return fmt.Errorf("failed to run ltgen: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to run ltgen: %w", err)
	}
	return nil
}
