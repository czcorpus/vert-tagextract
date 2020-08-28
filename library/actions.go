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
	"os/signal"
	"syscall"
	"time"

	"github.com/czcorpus/vert-tagextract/cnf"
	"github.com/czcorpus/vert-tagextract/db"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/fs"
	"github.com/czcorpus/vert-tagextract/proc"

	"github.com/tomachalek/vertigo/v4"
)

func sendErrStatusAndClose(statusChan chan proc.Status, file string, err error) {
	statusChan <- proc.Status{
		Datetime: time.Now(),
		File:     file,
		Error:    err,
	}
	close(statusChan)
}

// ExtractData extracts structural and/or positional attributes from a vertical file
// based on the specification in the 'conf' argument.
// The 'stopChan' can be used to handle calling service shutdown.
// The 'statusChan' is for getting extraction status information including possible errors
func ExtractData(conf *cnf.VTEConf, appendData bool, stopChan chan struct{}, statusChan chan proc.Status) {

	_, ferr := os.Stat(conf.DBFile)
	if os.IsNotExist(ferr) && appendData {
		err := fmt.Errorf("Update flag is set but the database %s does not exist", conf.DBFile)
		sendErrStatusAndClose(statusChan, conf.DBFile, err)
		return
	}

	if !appendData {
		log.Printf("The database file %s already exists. Existing data will be deleted.", conf.DBFile)
	}

	dbConn := db.OpenDatabase(conf.DBFile)
	defer dbConn.Close()

	if !appendData {
		if !os.IsNotExist(ferr) {
			db.DropExisting(dbConn)
		}
		db.CreateSchema(dbConn, conf.Structures, conf.IndexedCols, conf.UsesSelfJoin(), conf.Ngrams.AttrColumns)
		if conf.HasConfiguredBib() {
			db.CreateBibView(dbConn, conf.BibView.Cols, conf.BibView.IDAttr)
		}
	}

	var filesToProc []string
	if fs.IsFile(conf.VerticalFile) {
		filesToProc = []string{conf.VerticalFile}

	} else if fs.IsDir(conf.VerticalFile) {
		var err error
		filesToProc, err = fs.ListFilesInDir(conf.VerticalFile)
		if err != nil {
			sendErrStatusAndClose(statusChan, conf.VerticalFile, err)
			return
		}
	}

	for _, verticalFile := range filesToProc {
		parserConf := &vertigo.ParserConf{
			InputFilePath:         verticalFile,
			StructAttrAccumulator: "nil",
			Encoding:              conf.Encoding,
		}

		var fn colgen.AlignedColGenFn
		if conf.UsesSelfJoin() {
			fn = func(args map[string]interface{}) string {
				return colgen.GetFuncByName(conf.SelfJoin.GeneratorFn)(args, conf.SelfJoin.ArgColumns)
			}
		}

		subStatusChan := make(chan proc.Status, 10)
		go func() {
			for upd := range subStatusChan {
				upd.File = verticalFile
				statusChan <- upd
			}
		}()
		signalChan := make(chan os.Signal)
		signal.Notify(signalChan, os.Interrupt)
		signal.Notify(signalChan, syscall.SIGTERM)
		tte, err := proc.NewTTExtractor(dbConn, conf, fn, stopChan, subStatusChan)
		if err != nil {
			sendErrStatusAndClose(statusChan, "", err)
			return
		}
		err = tte.Run(parserConf)
		if err != nil {
			sendErrStatusAndClose(statusChan, verticalFile, err)
			return
		}
	}
	close(statusChan)
}
