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

package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/czcorpus/vert-tagextract/db"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/vteconf"
	"github.com/tomachalek/vertigo"
)

func main() {
	updateOnly := flag.Bool("update", false, "Update an existing schema, do not delete existing rows (useful e.g. for Intercorp)")
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatal("Unknown command, a config file must be specified")
	}
	conf := vteconf.LoadConf(flag.Arg(0))

	_, ferr := os.Stat(conf.DBFile)
	if os.IsNotExist(ferr) {
		if *updateOnly {
			log.Fatalf("Update flag is set but the database %s does not exist", conf.DBFile)
		}

	} else if !*updateOnly {
		log.Printf("The database file %s already exists. Existing data will be deleted.", conf.DBFile)
	}
	dbConn := db.OpenDatabase(conf.DBFile)

	if !*updateOnly {
		db.DropExisting(dbConn)
		db.CreateSchema(dbConn, conf)
		if conf.HasConfiguredBib() {
			db.CreateBibView(dbConn, conf)
		}
	}

	parserConf := &vertigo.ParserConf{
		VerticalFilePath:      conf.VerticalFile,
		StructAttrAccumulator: "nil",
	}

	var fn colgen.AlignedColGenFn
	if conf.UsesSelfJoin() {
		fn = func(args map[string]interface{}) string {
			return colgen.GetFuncByName(conf.SelfJoin.GeneratorFn)(args, conf.SelfJoin.ArgColumns)
		}
	}

	tte := db.NewTTExtractor(dbConn, conf.Corpus, conf.AtomStructure, conf.Structures, fn)
	t0 := time.Now()
	tte.Run(parserConf)
	log.Printf("Finished in %s seconds.\n", time.Since(t0))
}
