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

package library

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v3/cnf"
	"github.com/czcorpus/vert-tagextract/v3/db/colgen"
	"github.com/czcorpus/vert-tagextract/v3/db/factory"
	"github.com/czcorpus/vert-tagextract/v3/fs"
	"github.com/czcorpus/vert-tagextract/v3/proc"

	"github.com/tomachalek/vertigo/v6"
)

func sendErrStatus(statusChan chan proc.Status, file string, err error) {
	statusChan <- proc.Status{
		Datetime: time.Now(),
		File:     file,
		Error:    err,
	}
}

// determineLineReportingStep
// note: the numbers 0.02, 20 are just rough empirical values to determine
// number of lines based on "average" CNC corpus
func determineLineReportingStep(filePath string) int {
	size := fs.FileSize(filePath)
	tmp := float64(size) * 0.02
	if strings.HasSuffix(filePath, ".gz") || strings.HasSuffix(filePath, ".tgz") {
		tmp *= 20
	}
	step := 100
	for ; step < 1000000000; step *= 10 {
		if float64(size)/float64(step) < 10 {
			break
		}
	}
	return step
}

// ExtractData extracts structural and/or positional attributes from a vertical file
// based on the specification in the 'conf' argument.
// The returned status channel is for getting extraction status information including possible errors
func ExtractData(ctx context.Context, conf *cnf.VTEConf, appendData bool) (chan proc.Status, error) {
	if err := conf.Ngrams.UpgradeLegacy(); err != nil {
		return nil, fmt.Errorf("failed to process file: %w", err)
	}
	statusChan := make(chan proc.Status)
	dbWriter, err := factory.NewDatabaseWriter(conf)
	if err != nil {
		return nil, err
	}
	dbExisted := dbWriter.DatabaseExists()
	if !dbExisted && appendData {
		err := fmt.Errorf("update flag is set but the database %s does not exist", conf.DB.Name)
		return nil, err
	}

	var filesToProc []string
	if conf.VerticalFile != "" && len(conf.VerticalFiles) > 0 {
		return nil, fmt.Errorf("cannot use verticalFile and verticalFiles at the same time")
	}
	if conf.VerticalFile != "" && (fs.IsFile(conf.VerticalFile) || strings.HasPrefix(conf.VerticalFile, "|")) {
		filesToProc = []string{conf.VerticalFile}

	} else if conf.VerticalFile != "" && fs.IsDir(conf.VerticalFile) {
		var err error
		filesToProc, err = fs.ListFilesInDir(conf.VerticalFile)
		if err != nil {
			return nil, err
		}

	} else if len(conf.VerticalFiles) > 0 && fs.AllFilesExist(conf.VerticalFiles) {
		filesToProc = conf.VerticalFiles

	} else {
		return nil, fmt.Errorf("neither verticalFile nor verticalFiles provide a valid data source")
	}

	go func() {
		defer dbWriter.Close()
		defer close(statusChan)
		var wg sync.WaitGroup
		wg.Add(len(filesToProc))

		err := dbWriter.Initialize(appendData)
		if err != nil {
			wg.Done()
			sendErrStatus(statusChan, "", err)
			return
		}
		for _, verticalFile := range filesToProc {
			log.Info().Str("vertical", verticalFile).Msg("Processing vertical")
			parserConf := &vertigo.ParserConf{
				InputFilePath:         verticalFile,
				StructAttrAccumulator: "nil",
				Encoding:              conf.Encoding,
				LogProgressEachNth:    determineLineReportingStep(verticalFile),
			}

			var fn colgen.AlignedColGenFn
			if conf.SelfJoin.IsConfigured() {
				fn = func(args map[string]interface{}) (ident string, err error) {
					var colgenFn colgen.AlignedUnboundColGenFn
					defer func() {
						if r := recover(); r != nil {
							ident = ""
							err = fmt.Errorf("%v", r)
						}
					}()
					colgenFn, err = colgen.GetFuncByName(conf.SelfJoin.GeneratorFn)
					if err != nil {
						return
					}
					ident, err = colgenFn(args, conf.SelfJoin.ArgColumns)
					return
				}
			}

			subStatusChan := make(chan proc.Status, 10)
			go func() {
				defer wg.Done()
				for upd := range subStatusChan {
					upd.File = verticalFile
					statusChan <- upd
				}
			}()
			tte, err := proc.NewTTExtractor(ctx, dbWriter, conf, fn, subStatusChan)
			if err != nil {
				close(subStatusChan)
				sendErrStatus(statusChan, "", err)
			}
			err = tte.Run(parserConf)
			close(subStatusChan)
			if err != nil {
				sendErrStatus(statusChan, verticalFile, err)
			}
		}
		wg.Wait()
		err = dbWriter.Commit()
		if err != nil {
			sendErrStatus(statusChan, "", err)
		}
	}()

	return statusChan, nil
}
