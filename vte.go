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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/czcorpus/vert-tagextract/db"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/vteconf"
	"github.com/tomachalek/vertigo"
)

func dumpNewConf(dstPath string) {
	conf := vteconf.VTEConf{}
	conf.Encoding = "UTF-8"
	conf.AtomStructure = "p"
	conf.Structures = make(map[string][]string)
	conf.Structures["doc"] = []string{"id", "title"}
	conf.Structures["p"] = []string{"id", "type"}
	conf.IndexedCols = []string{}
	conf.BibView.Cols = []string{"doc_id", "doc_title", "doc_author", "doc_publisher"}
	conf.BibView.IDAttr = "doc_id"
	conf.SelfJoin.ArgColumns = []string{}
	b, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		log.Fatalf("Failed to dump a new config: %s", err)
	}
	f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		log.Fatalf("Failed to open file %s for writing", dstPath)
	}
	defer f.Close()
	f.Write(b)
}

func exportData(confPath string, updateOnly bool) {
	conf := vteconf.LoadConf(confPath)

	_, ferr := os.Stat(conf.DBFile)
	if os.IsNotExist(ferr) {
		if updateOnly {
			log.Fatalf("Update flag is set but the database %s does not exist", conf.DBFile)
		}

	} else if !updateOnly {
		log.Printf("The database file %s already exists. Existing data will be deleted.", conf.DBFile)
	}
	dbConn := db.OpenDatabase(conf.DBFile)

	if !updateOnly {
		db.DropExisting(dbConn)
		db.CreateSchema(dbConn, conf)
		if conf.HasConfiguredBib() {
			db.CreateBibView(dbConn, conf)
		}
	}

	parserConf := &vertigo.ParserConf{
		VerticalFilePath:      conf.VerticalFile,
		StructAttrAccumulator: "nil",
		Encoding:              conf.Encoding,
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
	log.Printf("Finished in %s.\n", time.Since(t0))
}

func main() {
	updateOnly := flag.Bool("update", false, "Update an existing schema, do not delete existing rows (useful e.g. for Intercorp)")
	flag.Usage = func() {
		fmt.Println("\n+-------------------------------------------------------------+")
		fmt.Println("| Vert-tagextract (vte) - a program for extracting structural |")
		fmt.Println("|            meta-data from a corpus vertical file            |")
		fmt.Println("|                         version 0.1                         |")
		fmt.Println("|          (c) Institute of the Czech National Corpus         |")
		fmt.Println("+-------------------------------------------------------------+")
		fmt.Printf("\nSupported encodings:\n%s\n", strings.Join(vertigo.SupportedCharsets(), ", "))
		fmt.Printf("\nSupported selfJoin column generator functions:\n%s\n", strings.Join(colgen.GetFuncList(), ", "))
		fmt.Println("\nUsage:")
		fmt.Println("vte create config.json -- run an export configured in config.json")
		fmt.Println("vte template config.json -- create a half empty sample config config.json")
		fmt.Println("(config file should be named after a respective corpus name, e.g. syn_v4.json)")

		fmt.Println("\nOptions:")
		flag.PrintDefaults()
	}

	flag.Parse()
	if len(flag.Args()) == 2 {
		switch flag.Arg(0) {
		case "create":
			exportData(flag.Arg(1), *updateOnly)
		case "template":
			dumpNewConf(flag.Arg(1))
		default:
			log.Fatalf("Unknown command '%s'", flag.Arg(0))
		}

	} else {
		log.Fatal("Unknown arguments, an action and a config file must be specified (use -h for help)")
	}

}
