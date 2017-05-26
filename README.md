# Vert-tagextract

Vert-tagextract (vte) is a simple program for extracting structural attribute meta-data
from a [corpus vertical file](https://www.sketchengine.co.uk/documentation/preparing-corpus-text/)
for use with corpus search interface [KonText](https://github.com/czcorpus/kontext).

## Preparing the process

To prepare data extraction from a specific corpus, a configuration file must be defined:

```json
{
    "corpus": "syn_v4",
    "verticalFile": "/path/to/vertical/file",
    "dbFile": "/var/opt/kontext/metadata/syn_v4.db",
    "encoding": "utf-8",
    "atomStructure": "p",
    "selfJoin": {
        "column": "text_id",
        "normalizeFn": "identity"
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

* *indexedCols* can be used to specify additional indices for columns expected to be used heavily
* *selfJoin* defines a column used to join rows belonging to different corpora (this is used mainly
  with the InterCorp);

## Running the export process

```
vte path/to/config.json
```

Or in case we want to add multiple corpora to a single database
(e.g. in case of InterCorp):

```
vte path/to/config1.json
vte -update path/to/config2.json
vte -update path/to/config3.json
...
vte -update path/to/configN.json
```