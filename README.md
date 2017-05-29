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

A fully fledged configuration file may look like this:

```json
{
    "corpus": "syn_v4",
    "verticalFile": "/path/to/vertical/file",
    "dbFile": "/var/opt/kontext/metadata/syn_v4.db",
    "encoding": "utf-8",
    "atomStructure": "p",
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
        ],
        "p" : [
            "id",
            "type"
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

Notes:

### indexedCols

This setting can be used to specify additional indices for columns expected to be used heavily

### selfJoin

This setting defines a column used to join rows belonging to different corpora (this is used mainly
with the InterCorp). Argument *generatorFn* contains an identifier of an internal function *vte*
uses to generate column names (current options are: *empty*, *identity* and *intercorp*).
Argument *argColumns* contains a list of attributes used as arguments to the *generatorFn*.

E.g. in case we want to create a compound *item_id* identifier from *doc.id*, *text.id* and *p.id*
we can define *"generatorFn" = "identity"* and  *"argColumns" = ["doc_id", "text_id", "p_id"]*.

## Running the export process

```
vte create path/to/config.json
```

Or in case we want to add multiple corpora to a single database
(e.g. in case of InterCorp):

```
vte create path/to/config1.json
vte create -update path/to/config2.json
vte create -update path/to/config3.json
...
vte create -update path/to/configN.json
```
