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
	"fmt"
	"os"
	"path/filepath"

	"github.com/czcorpus/rexplorer/parser"
)

// FindAttrColumn parses a Manatee corpus registry file and returns the
// vertical file column index (in the same 0-based indexing vertigo uses,
// where 0 is the word itself) of the static positional attribute attrName.
func FindAttrColumn(registryPath, attrName string) (int, error) {
	data, err := os.ReadFile(registryPath)
	if err != nil {
		return -1, fmt.Errorf("failed to read registry: %w", err)
	}
	doc, err := parser.ParseRegistryBytes(filepath.Base(registryPath), data)
	if err != nil {
		return -1, fmt.Errorf("failed to parse registry: %w", err)
	}
	attrs := doc.GetStaticPosattrs()
	// registries are expected to declare "word" as the first positional
	// attribute (i.e. vertical column 0); if it is missing, it is still
	// implicitly present in the vertical file, so the remaining attributes
	// need to be shifted by one column
	colOffset := 1
	if len(attrs) > 0 && attrs[0].Name == "word" {
		colOffset = 0
	}
	for i, a := range attrs {
		if a.Name == attrName {
			return i + colOffset, nil
		}
	}
	return -1, fmt.Errorf("attribute %q not found in registry %s", attrName, registryPath)
}
