This is a horribly put together early proof of concept of the nzbX V2 public indexing engine that is being developed as a result of the nzbX closure. It requires Python 3 and MongoDB installed. Python modules required include PyMongo and uh... well, that's it. It currently only checks against a single regex, which has only been tested in a single group.

**Installation**

After installing the pre-requirements, run the following commands in a Mongo shell.

use indexer

var g = { group: 'alt.binaries.teevee', first: 0, last: 0 }

db.groups.insert(g)

-----

Change the username and password of the NNTP host in index.py

Create a folder named nzbs with write permissions.

Run with python index.py and watch the flames. This is currently a performance test.