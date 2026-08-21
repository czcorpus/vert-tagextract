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
	"bufio"
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/tomachalek/vertigo/v6"
)

// PosTagsetGen extracts unique positional tagsets from a vertical file
type PosTagsetGen struct {
	ctx       context.Context
	data      map[string]int64
	tagColIdx int
}

// WriteValues writes all the collected unique tag values, sorted
// alphabetically, one value per line, to the provided writer.
func (ltg *PosTagsetGen) WriteValues(w io.Writer) error {
	values := make([]string, 0, len(ltg.data))
	for v := range ltg.data {
		values = append(values, v)
	}
	slices.Sort(values)
	bw := bufio.NewWriter(w)
	for _, v := range values {
		if _, err := fmt.Fprintln(bw, v); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}
	return bw.Flush()
}

func (ltg *PosTagsetGen) ProcToken(tk *vertigo.Token, line int, err error) error {
	value := tk.PosAttrByIndex(ltg.tagColIdx)
	ltg.data[value]++
	return nil
}

func (ltg *PosTagsetGen) ProcStruct(st *vertigo.Structure, line int, err error) error {
	return nil
}

func (ltg *PosTagsetGen) ProcStructClose(st *vertigo.StructureClose, line int, err error) error {
	return nil
}

func ParseFile(ctx context.Context, vertPath string, tagColIdx int, encoding string) (*PosTagsetGen, error) {
	parserConf := &vertigo.ParserConf{
		StructAttrAccumulator: "nil",
		Encoding:              encoding,
		LogProgressEachNth:    250000,
		InputFilePath:         vertPath,
	}
	proc := &PosTagsetGen{
		ctx:       ctx,
		data:      make(map[string]int64),
		tagColIdx: tagColIdx,
	}
	if err := vertigo.ParseVerticalFile(ctx, parserConf, proc); err != nil {
		return nil, fmt.Errorf("failed to parse vertical file: %w", err)
	}
	return proc, nil
}
