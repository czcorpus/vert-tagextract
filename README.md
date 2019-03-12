# Vert-tagextract

Vert-tagextract (vte) is a simple program for extracting structural attribute meta-data
and (optionally) PoS tag variants from a [corpus vertical file](https://www.sketchengine.co.uk/documentation/preparing-corpus-text/)
for use with corpus search interface [KonText](https://github.com/czcorpus/kontext).

* [Preparing the process](#preparing_the_process)
  * [Example config file](#example_config)
* [Configuration items](#configuration_items)
  * [verticalFile](#conf_verticalFile)
  * [dbFile](#conf_dbFile)
  * [atomStructure](#conf_atomStructure)
  * [stackStructEval](#conf_stackStructEval)
  * [structures](#conf_structures)
  * [indexedCols](#conf_indexedCols)
  * [selfJoin](#conf_selfJoin)
  * [bibView](#conf_bibView)
  * [posTagColumn](#conf_postagcolumn)
* [Running the export process](#running_the_export_process)

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
    "countColumns": [0, 1, 3]
}
```

## Configuration items
<a name="configuration_items"></a>

<a name="conf_verticalFile"></a>
### verticalFile

type: *string*

a path to a vertical file (plain text or *gz*)

<a name="conf_dbFile"></a>
### dbFile

type: *string*

a path of the sqlite3 database file the metadata will be exported to

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

<a name="conf_postagcolumn"></a>
### posTagColumn

type: *number*

If a value greater than zero is provided, then *vte* will also extract PoS tag
information along with number of occurrences of each variant. The value must
represent a column index (starting from zero) within a respective vertical file.
E.g. in case of *word, lemma, tag* columns, the value is *2*.

The data are stored into a separate table *postag*. This can be used to generate
lists of tags for KonText's *taghelper* plug-in. For this purpose,
script *scripts/postag2file.py* is available:

```
python scripts/postag2file.py path/to/generated/database
```

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
