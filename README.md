# Vert-tagextract

Vert-tagextract (vte) is a program for **extracting structural attribute meta-data**
and **word frequency** information (ipm, ARF)
from a [corpus vertical file](https://www.sketchengine.co.uk/documentation/preparing-corpus-text/)
to an **SQL database**.

The meta-data database part is used by [KonText](https://github.com/czcorpus/kontext) for its *liveattrs* plug-in.
The complete word frequency database is used by [Word at a Glance](https://github.com/czcorpus/wdglance) but it
can be used by anyone interested in n-gram analysis.

- [Vert-tagextract](#vert-tagextract)
  - [Preparing the process](#preparing-the-process)
    - [Example config](#example-config)
  - [Configuration items](#configuration-items)
    - [verticalFile](#verticalfile)
    - [db](#db)
    - [atomStructure](#atomstructure)
    - [stackStructEval](#stackstructeval)
    - [structures](#structures)
    - [indexedCols](#indexedcols)
    - [selfJoin](#selfjoin)
    - [bibView](#bibview)
    - [countColumns](#countcolumns)
    - [countColMod](#countcolmod)
    - [calcARF](#calcarf)
    - [filter](#filter)
  - [Running the export process](#running-the-export-process)

## Preparing the process
<a name="preparing_the_process"></a>

To prepare data extraction from a specific corpus, a configuration file must be defined. You can
start by generating a config template:

```
vte template > syn_v4.json
```

### Example config
<a name="example_config"></a>

An example configuration file written for corpus *syn_v4* looks like this:

```json
{
    "corpus": "syn_v4",
    "verticalFile": "/path/to/vertical/file",
    "dbFile": "/var/opt/kontext/metadata/syn_v4.db",
    "encoding": "utf-8",
    "atomStructure": "text",
    "stackStructEval": true,
    "selfJoin": {
        "argColumns": ["doc_id", "text_id"],
        "generatorFn": "identity"
    },
    "structures": {
        "doc" : [
            "id",
            "title",
            "subtitle",
            "author",
            "issue",
            "publisher"
        ],
        "text": [
            "id",
            "section",
            "section_orig",
            "author"
        ]
    },
    "indexedCols": ["doc_title"],
    "bibView" : {
        "cols" : [
            "doc_id",
            "doc_title",
            "doc_author"
        ],
        "idAttr" : "doc_id"
    },
    "countColumns": [0, 1, 3],
    "countColMod": ["toLower", "toLower", "firstChar"],
    "calcARF": true
}
```

## Configuration items
<a name="configuration_items"></a>

<a name="conf_verticalFile"></a>
### verticalFile

type: *string*

a path to a vertical file (plain text or *gz*)

<a name="conf_db"></a>
### db

type: *object*

attributes:

* `type: 'sqlite'|'mysql'`
* `name: string`
* `host: string`
* `user: string`
* `password: string`
* `preconfSettings: Array<string>`

<a name="conf_atomStructure"></a>
### atomStructure

type: *string*

This setting specifies a structure understood as a row in the exported metadata database. It means
that any nested structures (e.g. *p* within *text*) will be ignored. On the other hand, all the
ancestor structures (e.g. *doc* in case of *text*) will be processed as long as there are some
configured structural attributes to be exported (see the example above).

<a name="conf_stackStructEval"></a>
### stackStructEval

type: *boolean*

When *true* then structures within a vertical file are evaluated by a stack-based processor
which requires the sturctures to be nested properly (e.g. just like in case of XML). If
*false* then overlapping structures can be in the vertical file:

```sgml
<foo>
token1
<bar>
token2
</foo>
token3
</bar>
```

In case you are not sure about your vertical file structure, use *false*.

<a name="conf_structures"></a>
### structures

type: *{[key:string]:Array\<string\>}*

An object containing structures and their respective attributes
to be exported. Generally, this should be a superset of values found in a respective corpus
registry file under the *SUBCORPATTRS* key.

<a name="conf_indexedCols"></a>
### indexedCols

type: *Array\<string\>*

By defualt, *vte* creates indices for primary keys and for the *item_id* (see *selfJoin*) column
(if defined). In case of a large database it may be a good idea to create additional
indices for frequently accessed columns (e.g. a title of a document, genre etc.).

Please note that the format of structural attribute name matches the metadata column name
format (e.g. *doc_title* instead of *doc.title*).

<a name="conf_selfJoin"></a>
### selfJoin

type: *{argColumns: Array\<string\>; generatorFn: string}*

This setting defines a column used to join rows belonging to different corpora (this is used mainly
with the InterCorp). Argument *generatorFn* contains an identifier of an internal function *vte*
uses to generate column names (current options are: *empty*, *identity* and *intercorp*).
Argument *argColumns* contains a list of attributes used as arguments to the *generatorFn*.

E.g. in case we want to create a compound *item_id* identifier from *doc.id*, *text.id* and *p.id*
we can define *"generatorFn" = "identity"* and  *"argColumns" = ["doc_id", "text_id", "p_id"]*.
The column format is purely internal matter of KonText - the important thing is to match columns
properly and make the (*corpus_id*, *item_id*) pair unique.

<a name="conf_bibView"></a>
### bibView

type: *{idAttr: string; cols: Array\<string\>}*

This setting defines a database view used to fetch detail about a single "bibliographic unit"
(e.g. a book). This is optional as it may not apply for some cases (e.g. spoken corpora).

    * *idAttr* specifies an unique column to access the "bibliographic unit"
    * *cols* specifies columns displayed in bibliographic unit detail

Please note (again) the format of column names (*doc_title*, not *doc.title*).

<a name="conf_countColumns"></a>
### countColumns

type: *Array&lt;number&gt;*

If a non-empty array is provided, then *vte* will also extract the defined
columns (referred by their position starting from left and indexed from zero)
along with number of occurrences of each variant (i.e. all the unique combinations
for defined columns - e.g. "word"+"lemma"+"pos" and their respective absolute frequencies).

The data are stored into a separate table *colcounts*.

This can be used e.g. to generate lists of unique PoS tags for KonText's *taghelper* plug-in.
For this purpose, script *scripts/postag2file.py* is available:

```
python scripts/postag2file.py path/to/generated/database
```

<a name="conf_countColMod"></a>
### countColMod

type: *Array&lt;string|null&gt;*

It is also possible to define value modification function(s) per individual
extracted token columns. Full length of *countColumns* must be used. Columns
without value modifications should contain *null*.

Available functions: *toLower*, *firstChar*, null (= identity is used)


<a name="conf_calcARF"></a>
### calcARF

type: boolean

If true and if *countColumns* is also defined then *vte* will also calculate
[ARF](http://wiki.korpus.cz/doku.php/en:pojmy:arf). Such a calculation requires
a 2nd pass of the vertical file so the whole process consumes roughly twice
as much time compared with non-ARF processing.

<a name="conf_filter"></a>
### filter

type: *{lib:string; fn:string}*

Specifies a path to a compiled plug-in library along with exported variable
implementing *LineFilter* interface. It is used as a filter for each token
where input is given by current structural attributes and their respective
values. This can be used to process just a predefined subcorpus of the original
corpus.

<a name="running_the_export_process"></a>
## Running the export process

To create a new or replace an existing database use:

```
vte create path/to/config.json
```

Or in case we want to add multiple corpora to a single database
(e.g. in case of *InterCorp*):

```
vte create path/to/config1.json
vte append path/to/config2.json
vte append path/to/config3.json
...
vte append path/to/configN.json
```

In this case, a proper *selfJoin* must be configured for KonText to be able to
match rows from different corpora as aligned ones.
