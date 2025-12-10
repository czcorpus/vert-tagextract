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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiFileScanner_ScanAndText(t *testing.T) {
	tmpDir := t.TempDir()

	file1Path := filepath.Join(tmpDir, "file1.txt")
	file2Path := filepath.Join(tmpDir, "file2.txt")

	file1Content := "line1\nline2\nline3\n"
	file2Content := "line4\nline5\n"

	err := os.WriteFile(file1Path, []byte(file1Content), 0644)
	assert.NoError(t, err, "Failed to create test file1")

	err = os.WriteFile(file2Path, []byte(file2Content), 0644)
	assert.NoError(t, err, "Failed to create test file2")

	scanner, err := NewMultiFileScanner(file1Path, file2Path)
	assert.NoError(t, err, "Failed to create MultiFileScanner")
	defer scanner.Close()

	expectedLines := []string{"line1", "line2", "line3", "line4", "line5"}
	lines := []string{}

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	assert.NoError(t, scanner.Err(), "Scanner should not return an error")
	assert.Equal(t, expectedLines, lines, "Scanner should read all lines from both files in order")
}
