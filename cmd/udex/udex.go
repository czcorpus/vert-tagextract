// Copyright 2021 Martin Zimandl <martin.zimandl@gmail.com>
// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2024 Charles University, Faculty of Arts,
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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/czcorpus/cnc-gokit/collections"
)

var (
	version   string
	build     string
	gitCommit string
)

type feat [2]string

func (f feat) Key() string {
	return f[0]
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func printMsg(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}

type tokenFeats []feat

func (tf tokenFeats) String() string {
	return fmt.Sprintf("%v", []feat(tf))
}

type tokenMVFeats struct {
	value []tokenFeats
	hash  uint64
}

func (tf *tokenMVFeats) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(tf.value)
}

func (tf *tokenMVFeats) Hash() uint64 {
	if tf.hash == 0 {
		var buff strings.Builder
		for _, x := range tf.value {
			for _, x2 := range x {
				buff.WriteString(x2[0] + x2[1])
			}
		}
		tf.hash = hashString(buff.String())
	}
	return tf.hash
}

func (tf *tokenMVFeats) Compare(other collections.Comparable) int {
	s1 := tf.Hash()
	sOther, ok := other.(*tokenMVFeats)
	if !ok {
		return -1
	}
	return int(s1 - sOther.Hash())
}

func getPosMultiValue(s string) []string {
	return strings.Split(s, "|")
}

func getFeatMultiValue(s string) []string {
	return strings.Split(s, "||")
}

func parseFeats(s string) (tokenFeats, error) {
	items := strings.Split(s, "|")
	ans := make([]feat, 0, len(items)+1) // +1 is for PoS added by the caller
	for _, item := range items {
		tmp := strings.SplitN(item, "=", 2)
		if len(tmp) == 0 || item == "" {
			return tokenFeats{}, nil
		}
		if len(tmp) == 1 {
			return tokenFeats{}, fmt.Errorf("unparseable feature '%s'", item)
		}
		if tmp[0] == "_" {
			continue
		}
		ans = append(ans, feat{tmp[0], tmp[1]})
	}
	return ans, nil
}

func parseVerticalLine(line string, posIdx, featIdx int, analyzer *analyzer) *tokenMVFeats {
	analyzer.SetNewLine()
	positions := strings.Split(line, "\t")
	posInfo := getPosMultiValue(positions[posIdx])
	for _, v := range posInfo {
		analyzer.AddPos(v)
	}
	feats := getFeatMultiValue(positions[featIdx])
	if len(posInfo) != len(feats) {
		analyzer.AddNamedError(
			fmt.Sprintf(
				"unequal number of multi-value items for PoS and feats: %s ... %s",
				posInfo, feats,
			),
		)
		return &tokenMVFeats{}
	}
	ans := &tokenMVFeats{
		value: make([]tokenFeats, len(posInfo)),
	}
	for i := 0; i < len(posInfo); i++ {
		pFeats, err := parseFeats(feats[i])
		if err != nil {
			analyzer.AddNamedError(err.Error())
		}
		for _, v := range pFeats {
			analyzer.AddFeat(v.Key())
		}
		pFeats = append(pFeats, feat{"POS", posInfo[i]})
		sort.SliceStable(pFeats, func(i, j int) bool {
			return pFeats[i].Key() < pFeats[j].Key()
		})
		ans.value[i] = pFeats
	}
	return ans
}

func loadVariations(srcPath string, posIdx, featIdx int, analyzer *analyzer) ([]*tokenMVFeats, error) {

	f, err := os.Open(srcPath)
	if err != nil {
		return []*tokenMVFeats{}, fmt.Errorf("failed to load variations: %w", err)
	}
	variants := new(collections.BinTree[*tokenMVFeats])
	variants.UniqValues = true
	rdr := bufio.NewScanner(f)
	var lineNum int64
	for rdr.Scan() {
		lineNum++
		line := rdr.Text()
		if !strings.HasPrefix(line, "<") { // a line with structure definition
			feats := parseVerticalLine(line, posIdx, featIdx, analyzer)
			if analyzer.TooMuchErrors() {
				printMsg("too much errors, please make sure that correct columns are used")
				if analyzer.LastErr() != "" {
					printMsg("last error: ", analyzer.LastErr())
				}
				os.Exit(3)
			}
			variants.Add(feats)
		}
		if lineNum%1000 == 0 {
			printMsg("processed %d lines", lineNum)
		}
	}
	return variants.ToSlice(), nil
}

func askYesOrNo(q string) bool {
	rdr := bufio.NewReader(os.Stdin)
	for {
		printMsg("%s [y/n]: ", q)
		response, err := rdr.ReadString('\n')
		if err != nil {
			printMsg("ERROR: ", err)
			os.Exit(1)
			return false
		}
		response = strings.ToLower(strings.TrimSpace(response))
		switch response {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		}
	}
}

func main() {
	flag.Usage = func() {
		var verStr strings.Builder
		baseHdrRow := "+-------------------------------------------------------------+"
		verStr.WriteString(version)
		fmt.Printf("\n%s\n", baseHdrRow)
		fmt.Println("|     UD-extract (udex) - a program for extracting UD tags    |")
		fmt.Println("|               from a corpus vertical file                   |")
		fmt.Printf("|                version %s                    |\n", &verStr)
		fmt.Println("|      (c) Institute of the Czech National Corpus             |")
		fmt.Println("|     (c) Martin Zimandl <martin.zimandl@gmail.com>           |")
		fmt.Println("|     (c) Tomas Machalek <tomas.machalek@gmail.com>           |")
		fmt.Println("+-------------------------------------------------------------+")
		fmt.Println("\nUsage:")
		fmt.Println("udex [pos attr idx] [feat attr idx] [vertical path]")
		flag.PrintDefaults()
	}
	noChecks := flag.Bool("no-checks", false, "no previews, prompts and checks, just process the file")
	flag.Parse()
	posIdx, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		printMsg("cmd argument posIdx error: %w", err)
		os.Exit(1)
	}
	featIdx, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		printMsg("cmd argument featIdx error: %w", err)
		os.Exit(1)
	}

	if !*noChecks {
		if err := showSelectedFeats(flag.Arg(2), posIdx, featIdx); err != nil {
			printMsg("cannot show attr preview: %w", err)
			os.Exit(3)
		}
		if cont := askYesOrNo("does it look OK?"); !cont {
			os.Exit(5)
		}
	}
	t0 := time.Now()

	analyzer := newAnalyzer(*noChecks)
	feats, err := loadVariations(flag.Arg(2), posIdx, featIdx, analyzer)
	if err != nil {
		printMsg("failed to load variants: %w", err)
	}
	printMsg("proc. time: %01.2fs\n", time.Since(t0).Seconds())
	out, err := sonic.Marshal(feats)
	if err != nil {
		printMsg("failed to serialize result: %w", err)
		os.Exit(6)
	}
	fmt.Println(string(out))
}
