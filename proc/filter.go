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
	"fmt"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v3/fs"
	"github.com/tomachalek/vertigo/v6"
)

const (
	defaultSystemPluginDir = "/usr/local/lib/vert-tagextract"
)

// LineFilter allows selecting only tokens with specific
// accumulated structure information (e.g. I want doc.type='scifi' AND
// text.type!='meta').
type LineFilter interface {
	Apply(tk *vertigo.Token, attrAcc AttrAccumulator) bool
}

func findPluginLib(pathSuff string) (string, error) {
	paths := []string{
		pathSuff,
		filepath.Join(fs.GetWorkingDir(), pathSuff),
		filepath.Join(defaultSystemPluginDir, pathSuff),
	}
	for _, fullPath := range paths {
		if fs.IsFile(fullPath) {
			return fullPath, nil
		}
	}
	return "", fmt.Errorf("Failed to find plug-in file in %s", strings.Join(paths, ", "))
}

// PassAllFilter is the default filter which
// returns true for any struct-attr values.
type PassAllFilter struct{}

// Apply tests current state of the attribute accumulator against
// the filter.
func (df *PassAllFilter) Apply(tk *vertigo.Token, attrAcc AttrAccumulator) bool {
	return true
}

// LoadCustomFilter loads a compiled .so plugin from a defined
// path and selects a function identified by fn.
// In case libPath does not point to an existing file, the function
// handles it as a path suffix and tries other locations (working
// directory, /usr/local/lib/gloomy).
func LoadCustomFilter(libPath string, fn string) (LineFilter, error) {
	if libPath != "" && fn != "" {
		fullPath, err := findPluginLib(libPath)
		if err != nil {
			return nil, err
		}
		p, err := plugin.Open(fullPath)
		if err != nil {
			return nil, err
		}
		f, err := p.Lookup(fn)
		if err != nil {
			return nil, err
		}
		log.Info().
			Str("plugin", fn).
			Str("location", fullPath).
			Msg("Using a custom filter plug-in")
		return f.(LineFilter), nil
	}
	log.Info().Msg("No custom filter plug-in defined. Using 'pass all'")
	return &PassAllFilter{}, nil
}
