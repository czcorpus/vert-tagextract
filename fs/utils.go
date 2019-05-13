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

package fs

import "os"

// IsDir tests whether a provided path represents
// a directory. If not or in case of an IO error,
// false is returned.
func IsDir(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	finfo, err := f.Stat()
	if err != nil {
		return false
	}
	return finfo.Mode().IsDir()
}

// IsFile tests whether a provided path represents
// a file. If not or in case of an IO error,
// false is returned.
func IsFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	finfo, err := f.Stat()
	if err != nil {
		return false
	}
	return finfo.Mode().IsRegular()
}

// GetWorkingDir returns a program working
// directory. In case of an error, an empty
// string is returned.
func GetWorkingDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	return dir
}
