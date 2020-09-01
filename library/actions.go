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

	"github.com/czcorpus/vert-tagextract/cnf"
	"github.com/czcorpus/vert-tagextract/db"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/fs"
	"github.com/czcorpus/vert-tagextract/proc"

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

	if !fs.IsFile(conf.DBFile) && appendData {
		err := fmt.Errorf("Update flag is set but the database %s does not exist", conf.DBFile)
		return nil, err
	}

	dbConn, err := db.OpenDatabase(conf.DBFile)
	if err != nil {
		return nil, err
	}

	if !appendData {
		if fs.IsFile(conf.DBFile) {
			log.Printf("The database file %s already exists. Existing data will be deleted.", conf.DBFile)
			err := db.DropExisting(dbConn)
			if err != nil {
				return nil, err
			}
		}
		err := db.CreateSchema(dbConn, conf.Structures, conf.IndexedCols, conf.UsesSelfJoin(), conf.Ngrams.AttrColumns)
		if err != nil {
			return nil, err
		}
		if conf.HasConfiguredBib() {
			err := db.CreateBibView(dbConn, conf.BibView.Cols, conf.BibView.IDAttr)
			if err != nil {
				return nil, err
			}
		}
	}

	var filesToProc []string
	if fs.IsFile(conf.VerticalFile) {
		filesToProc = []string{conf.VerticalFile}

	} else if fs.IsDir(conf.VerticalFile) {
		var err error
		filesToProc, err = fs.ListFilesInDir(conf.VerticalFile)
		if err != nil {
			return nil, err
		}
	}

	go func() {
		defer dbConn.Close()
		defer close(statusChan)
		var wg sync.WaitGroup
		wg.Add(len(filesToProc))
		for _, verticalFile := range filesToProc {
			parserConf := &vertigo.ParserConf{
				InputFilePath:         verticalFile,
				StructAttrAccumulator: "nil",
				Encoding:              conf.Encoding,
			}

			var fn colgen.AlignedColGenFn
			if conf.UsesSelfJoin() {
				fn = func(args map[string]interface{}) (string, error) {
					ans, err := colgen.GetFuncByName(conf.SelfJoin.GeneratorFn)
					if err != nil {

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
			tte, err := proc.NewTTExtractor(dbConn, conf, fn, subStatusChan, stopChan)
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
	}()

	return statusChan, nil
}
