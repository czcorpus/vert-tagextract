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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/czcorpus/vert-tagextract/cnf"
	"github.com/czcorpus/vert-tagextract/db/colgen"
	"github.com/czcorpus/vert-tagextract/library"

	"github.com/tomachalek/vertigo/v5"
)

var (
	version   string
	build     string
	gitCommit string
)

func dumpNewConf() {
	conf := cnf.VTEConf{}
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
	fmt.Print(string(b))
	fmt.Println()
}

func exportData(confPath string, appendData bool) {
	conf, err := cnf.LoadConf(confPath)
	if err != nil {
		log.Fatal("FATAL: ", err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	signal.Notify(signalChan, syscall.SIGTERM)

	t0 := time.Now()
	statusChan, err := library.ExtractData(conf, appendData, signalChan)
	if err != nil {
		log.Fatal("FATAL: ", err)
	}
	for status := range statusChan {
		if status.Error != nil {
			log.Print("ERROR: ", status.Error)
		}
	}
	log.Printf("Finished in %s.\n", time.Since(t0))
}

func main() {
	flag.Usage = func() {
		fmt.Println("\n+-------------------------------------------------------------+")
		fmt.Println("| Vert-tagextract (vte) - a program for extracting text types |")
		fmt.Println("|       and pos. attributes  from a corpus vertical file      |")
		fmt.Printf("|                       version %s                         |\n", version)
		fmt.Println("|          (c) Institute of the Czech National Corpus         |")
		fmt.Println("|         (c) Tomas Machalek tomas.machalek@ff.cuni.cz        |")
		fmt.Println("+-------------------------------------------------------------+")
		fmt.Printf("\nSupported encodings:\n%s\n", strings.Join(vertigo.SupportedCharsets(), ", "))
		fmt.Printf("\nSupported selfJoin column generator functions:\n%s\n", strings.Join(colgen.GetFuncList(), ", "))
		fmt.Println("\nUsage:")
		fmt.Println("vte create config.json\n\t(run an export configured in config.json, add data to a new database)")
		fmt.Println("vte append config.json\n\t(run an export configured in config.json, add data to an existing database)")
		fmt.Println("vte template\n\t(create a half empty sample config and write it to stdout)")
		fmt.Println("\n(config file should be named after a respective corpus name, e.g. syn_v4.json)")
		fmt.Println("vte version\n\tshow detailed version information")

		fmt.Println("\nOptions:")
		flag.PrintDefaults()
	}

	createCommand := flag.NewFlagSet("create", flag.ExitOnError)
	createCommand.Usage = func() {
		fmt.Println("Usage: vte create conf.json")
	}
	appendCommand := flag.NewFlagSet("append", flag.ExitOnError)
	appendCommand.Usage = func() {
		fmt.Println("Usage: vte append conf.json")
	}
	templateCommand := flag.NewFlagSet("template", flag.ExitOnError)
	templateCommand.Usage = func() {
		fmt.Println("Usage: vte template [> conf.json]")
	}
	flag.Parse()

	switch os.Args[1] {
	case "create":
		createCommand.Parse(os.Args[2:])
		exportData(createCommand.Arg(0), false)
	case "append":
		appendCommand.Parse(os.Args[2:])
		exportData(appendCommand.Arg(0), true)
	case "template":
		templateCommand.Parse(os.Args[2:])
		dumpNewConf()
	case "version":
		fmt.Printf("vert-tagextract %s\nbuild date: %s\nlast commit: %s\n", version, build, gitCommit)
	default:
		log.Fatalf("Unknown command '%s'", flag.Arg(0))
	}
}
