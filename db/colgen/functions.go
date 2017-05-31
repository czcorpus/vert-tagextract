// Copyright 2017 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2017 Charles University, Faculty of Arts,
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

package colgen

import (
	"log"
	"strings"
)

var (
	FuncList = map[string]func(map[string]interface{}, []string) string{
		"intercorp": intercorp,
		"identity":  identity,
		"empty":     empty,
	}
)

type AlignedColGenFn func(map[string]interface{}) string

type AlignedUnboundColGenFn func(map[string]interface{}, []string) string

func fetchStringVals(attrs map[string]interface{}, useAttrs []string) []string {
	ans := make([]string, len(useAttrs))
	for i, attr := range useAttrs {
		switch attrs[attr].(type) {
		case string:
			ans[i] = attrs[attr].(string)
		default:
			log.Fatalf("Column generator function cannot accept non-string values: %v (key: %s, type %T)", attrs[attr], attr, attrs[attr])
		}
	}
	return ans
}

func intercorp(attrs map[string]interface{}, useAttrs []string) string {
	vals := fetchStringVals(attrs, useAttrs)
	return vals[0][2:]
}

func empty(attrs map[string]interface{}, useAttrs []string) string {
	return ""
}

func identity(attrs map[string]interface{}, useAttrs []string) string {
	return strings.Join(fetchStringVals(attrs, useAttrs), "_")
}

func GetFuncByName(fnName string) AlignedUnboundColGenFn {
	fn, ok := FuncList[fnName]
	if ok {
		return fn
	}
	log.Fatalf("Unknown aligned column generator function: %s", fnName)
	return nil
}

func GetFuncList() []string {
	ans := make([]string, len(FuncList))
	i := 0
	for k := range FuncList {
		ans[i] = k
		i++
	}
	return ans
}
