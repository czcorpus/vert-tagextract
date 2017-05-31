# Vert-tagextract

Vert-tagextract (vte) is a simple program for extracting structural attribute meta-data
from a [corpus vertical file](https://www.sketchengine.co.uk/documentation/preparing-corpus-text/)
for use with corpus search interface [KonText](https://github.com/czcorpus/kontext).

## Preparing the process

To prepare data extraction from a specific corpus, a configuration file must be defined. You can
start by generating a config template:

```
vte template syn_v4.json
```

An example configuration file written for corpus *syn_v4* looks like this:

```json
{
    "corpus": "syn_v4",
    "verticalFile": "/path/to/vertical/file",
    "dbFile": "/var/opt/kontext/metadata/syn_v4.db",
    "encoding": "utf-8",
    "atomStructure": "text",
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
    }
}
```

Configuration items:

### verticalFile

type: *string*

a path to a vertical file (plain text or *gz*)

### dbFile

a path of the metadata database file

### atomStructure

type: *string*

This setting specifies a structure understood as a row in the exported metadata database. It means
that any nested structures (e.g. *p* within *text*) will be ignored. On the other hand, all the
ancestor structures (e.g. *doc* in case of *text*) will be processed as long as there are some
configured structural attributes to be exported (see the example above).


### structures

type: *{[key:string]:Array<string>}*

An object containing structures and their respective attributes
to be exported. Generally, this should be a superset of values found in a respective corpus
registry file under the *SUBCORPATTRS* key.

### indexedCols

type: *Array<string>*

By defualt, *vte* creates indices for primary keys and for the *item_id* (see *selfJoin*) column
(if defined). In case of a large database it may be a good idea to create additional
indices for frequently accessed columns (e.g. a title of a document, genre etc.).

Please note that the format of structural attribute name matches the metadata column name
format (e.g. *doc_title* instead of *doc.title*).

### selfJoin

type: *{argColumns: Array<string>; generatorFn: string}*

This setting defines a column used to join rows belonging to different corpora (this is used mainly
with the InterCorp). Argument *generatorFn* contains an identifier of an internal function *vte*
uses to generate column names (current options are: *empty*, *identity* and *intercorp*).
Argument *argColumns* contains a list of attributes used as arguments to the *generatorFn*.

E.g. in case we want to create a compound *item_id* identifier from *doc.id*, *text.id* and *p.id*
we can define *"generatorFn" = "identity"* and  *"argColumns" = ["doc_id", "text_id", "p_id"]*.
The column format is purely internal matter of KonText - the important thing is to match columns
properly and make the (*corpus_id*, *item_id*) pair unique.

### bibView

type: *{idAttr: string; cols: Array<string>}*

This setting defines a database view used to fetch detail about a single "bibliographic unit"
(e.g. a book). This is optional as it may not apply for some cases (e.g. spoken corpora).

    * *idAttr* specifies an unique column to access the "bibliographic unit"
    * *cols* specifies columns displayed in bibliographic unit detail

Please note (again) the format of column names (*doc_title*, not *doc.title*).

## Running the export process

To create a new or replace an existing database use:

```
vte create path/to/config.json
```

Or in case we want to add multiple corpora to a single database
(e.g. in case of InterCorp):

```
vte create path/to/config1.json
vte update path/to/config2.json
vte update path/to/config3.json
...
vte update path/to/configN.json
```

In this case, a proper *selfJoin* must be configured for KonText to be able to
match rows from different corpora as aligned ones.
