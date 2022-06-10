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
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/czcorpus/vert-tagextract/v2/cnf"
	"github.com/czcorpus/vert-tagextract/v2/db/colgen"
	"github.com/czcorpus/vert-tagextract/v2/db/factory"
	"github.com/czcorpus/vert-tagextract/v2/fs"
	"github.com/czcorpus/vert-tagextract/v2/proc"

	"github.com/tomachalek/vertigo/v5"
)

func sendErrStatus(statusChan chan proc.Status, file string, err error) {
	statusChan <- proc.Status{
		Datetime: time.Now(),
		File:     file,
		Error:    err,
	}
}

// ExtractData extracts structural and/or positional attributes from a vertical file
// based on the specification in the 'conf' argument.
// The 'stopChan' can be used to handle calling service shutdown.
// The 'statusChan' is for getting extraction status information including possible errors
func ExtractData(conf *cnf.VTEConf, appendData bool, stopChan <-chan os.Signal) (chan proc.Status, error) {
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
	if conf.VerticalFile != "" && fs.IsFile(conf.VerticalFile) {
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
			log.Printf("Processing vertical %s", verticalFile)
			parserConf := &vertigo.ParserConf{
				InputFilePath:         verticalFile,
				StructAttrAccumulator: "nil",
				Encoding:              conf.Encoding,
			}

			var fn colgen.AlignedColGenFn
			if conf.SelfJoin.IsConfigured() {
				fn = func(args map[string]interface{}) (string, error) {
					ans, err := colgen.GetFuncByName(conf.SelfJoin.GeneratorFn)
					if err != nil {
						return "", err
					}
					return ans(args, conf.SelfJoin.ArgColumns)
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
			tte, err := proc.NewTTExtractor(dbWriter, conf, fn, subStatusChan, stopChan)
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
		log.Print("...DONE")
	}()

	return statusChan, nil
}
