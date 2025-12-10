// Copyright 2025 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2025 Charles University, Faculty of Arts,
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
	"bufio"
	"fmt"
	"os"
)

// MultiFileScanner wraps multiple files and provides a unified scanning interface
type MultiFileScanner struct {
	filePaths    []string
	currentIndex int
	currentFile  *os.File
	scanner      *bufio.Scanner
	err          error
}

// NewMultiFileScanner creates a scanner that reads through multiple files sequentially
func NewMultiFileScanner(filePaths ...string) (*MultiFileScanner, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("at least one file path required")
	}

	mfs := &MultiFileScanner{
		filePaths:    filePaths,
		currentIndex: -1,
	}

	// Open the first file
	if !mfs.openNextFile() {
		return nil, mfs.err
	}

	return mfs, nil
}

func (mfs *MultiFileScanner) FilesID() string {
	if len(mfs.filePaths) > 0 {
		return fmt.Sprintf("multifile://%s", mfs.filePaths[0])
	}
	return "multifile://-"
}

// openNextFile opens the next file in the sequence
func (mfs *MultiFileScanner) openNextFile() bool {
	if mfs.currentFile != nil {
		mfs.currentFile.Close()
		mfs.currentFile = nil
		mfs.scanner = nil
	}
	mfs.currentIndex++
	if mfs.currentIndex >= len(mfs.filePaths) {
		return false
	}

	file, err := os.Open(mfs.filePaths[mfs.currentIndex])
	if err != nil {
		mfs.err = err
		return false
	}

	mfs.currentFile = file
	mfs.scanner = bufio.NewScanner(file)
	return true
}

// Scan advances to the next line, returning false when finished or on error
func (mfs *MultiFileScanner) Scan() bool {
	if mfs.scanner == nil {
		return false
	}

	if mfs.scanner.Scan() {
		return true
	}

	if err := mfs.scanner.Err(); err != nil {
		mfs.err = err
		return false
	}

	// Current file exhausted, try opening next file
	return mfs.openNextFile() && mfs.Scan()
}

// Text returns the current line
func (mfs *MultiFileScanner) Text() string {
	if mfs.scanner == nil {
		return ""
	}
	return mfs.scanner.Text()
}

// Bytes returns the current line as bytes
func (mfs *MultiFileScanner) Bytes() []byte {
	if mfs.scanner == nil {
		return nil
	}
	return mfs.scanner.Bytes()
}

// Err returns the first error encountered during scanning
func (mfs *MultiFileScanner) Err() error {
	return mfs.err
}

// Close closes any open file handles
func (mfs *MultiFileScanner) Close() error {
	if mfs.currentFile != nil {
		err := mfs.currentFile.Close()
		mfs.currentFile = nil
		mfs.scanner = nil
		return err
	}
	return nil
}
