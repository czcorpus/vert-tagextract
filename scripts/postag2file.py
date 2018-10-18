# Copyright 2018 Tomas Machalek <tomas.machalek@gmail.com>
# Copyright 2018 Charles University, Faculty of Arts,
#                Institute of the Czech National Corpus
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlite3
import sys

if __name__ == '__main__':
    db = sqlite3.connect(sys.argv[1])
    c = db.cursor()
    c.execute('SELECT value FROM postag ORDER BY value')
    for v in c.fetchall():
        print(v[0])