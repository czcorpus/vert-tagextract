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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/czcorpus/vert-tagextract/v2/cnf"
	"github.com/czcorpus/vert-tagextract/v2/db/colgen"
	"github.com/czcorpus/vert-tagextract/v2/library"

	"github.com/tomachalek/vertigo/v5"
)

var (
	version   string
	build     string
	gitCommit string
)

func dumpNewConf(corpusName string) {
	conf := cnf.VTEConf{
		Corpus: corpusName,
	}
	conf.Encoding = "UTF-8"
	conf.AtomStructure = "p"
	conf.Structures = make(map[string][]string)
	conf.Structures["doc"] = []string{"id", "title"}
	conf.Structures["p"] = []string{"id", "type"}
	conf.IndexedCols = []string{}
	conf.BibView.Cols = []string{"doc_id", "doc_title", "doc_author", "doc_publisher"}
	conf.BibView.IDAttr = "doc_id"
	conf.SelfJoin.ArgColumns = []string{}
	conf.VerticalFiles = []string{"./vertical"}
	b, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to dump a new config")
	}
	fmt.Print(string(b))
	fmt.Println()
}

func exportData(confPath string, appendData bool) error {
	conf, err := cnf.LoadConf(confPath)
	if err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	signal.Notify(signalChan, syscall.SIGTERM)

	t0 := time.Now()
	statusChan, err := library.ExtractData(conf, appendData, signalChan)
	if err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}
	for status := range statusChan {
		if status.Error != nil {
			log.Error().Err(status.Error).Msg("error during data extraction (not exiting)")
		}
	}
	log.Info().Dur("procTime", time.Since(t0)).Msg("Finished")
	return nil
}

func setupLog(jsonLog bool) {
	if !jsonLog {
		log.Logger = log.Output(
			zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: time.RFC3339,
			},
		)
	}
}

func main() {
	flag.Usage = func() {
		var verStr strings.Builder
		baseHdrRow := "+-------------------------------------------------------------+"
		verStr.WriteString(version)
		fmt.Printf("\n%s\n", baseHdrRow)
		fmt.Println("| Vert-tagextract (vte) - a program for extracting text types |")
		fmt.Println("|       and pos. attributes  from a corpus vertical file      |")
		fmt.Printf("|                       version %s|\n", &verStr)
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
	}
	flag.Parse()
	var jsonLog bool

	createCommand := flag.NewFlagSet("create", flag.ExitOnError)
	createCommand.BoolVar(&jsonLog, "json-log", false, "set JSON logging format")
	createCommand.Usage = func() {
		fmt.Println("Usage: vte create conf.json")
		fmt.Println("\nOptions:")
		createCommand.PrintDefaults()
	}
	appendCommand := flag.NewFlagSet("append", flag.ExitOnError)
	appendCommand.BoolVar(&jsonLog, "json-log", false, "set JSON logging format")
	appendCommand.Usage = func() {
		fmt.Println("Usage: vte append conf.json")
		fmt.Println("\nOptions:")
		createCommand.PrintDefaults()
	}
	templateCommand := flag.NewFlagSet("template", flag.ExitOnError)
	templateCommand.BoolVar(&jsonLog, "json-log", false, "set JSON logging format")
	templateCommand.Usage = func() {
		fmt.Println("Usage: vte template [> conf.json]")
		fmt.Println("\nOptions:")
		createCommand.PrintDefaults()
	}

	if len(os.Args) < 2 {
		fmt.Println("Action not specified")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "create":
		if len(os.Args) < 3 {
			fmt.Println("Missing argument")
			os.Exit(3)
		}
		createCommand.Parse(os.Args[2:])
		setupLog(jsonLog)
		if err := exportData(createCommand.Arg(0), false); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

	case "append":
		if len(os.Args) < 3 {
			fmt.Println("Missing argument")
			os.Exit(3)
		}
		appendCommand.Parse(os.Args[2:])
		setupLog(jsonLog)
		if err := exportData(appendCommand.Arg(0), true); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "template":
		if len(os.Args) < 3 {
			fmt.Println("Missing argument")
			os.Exit(3)
		}
		templateCommand.Parse(os.Args[2:])
		setupLog(jsonLog)
		dumpNewConf(templateCommand.Arg(0))
	case "version":
		fmt.Printf("vert-tagextract %s\nbuild date: %s\nlast commit: %s\n", version, build, gitCommit)
	default:
		log.Fatal().Msgf("Unknown command: %s", flag.Arg(0))
	}
}
