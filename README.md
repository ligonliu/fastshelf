# fastshelf

high performance persistent python3 dict with choices of backend

## Usage Example

```
from fastshelf import Shelf
import dbm.gnu
path = "/tmp/shelf1.gnudb"
shelf1 = Shelf(dbm.gnu.open(path,'cf'))

shelf1['a'] = 'A'
shelf1[(1,2,3)] = 'B'
shelf1[144] = {1,2,3,4,12}

print(dict(shelf1))
shelf1.close()

shelf1 = Shelf(dbm.gnu.open(path,'cf'))
print(dict(shelf1))
print(shelf1[(1,2,3)])

import shutil
shutil.rmtree(path)

shelf2 = Shelf(dbm.gnu.open(path,'cf'))
print(shelf2.get("non_existing_key"))  # output None
shelf2['a']='A'
print('a' in shelf2) # output True
del shelf2['a']
print('a' in shelf2) # output False
shelf2.update((i,i**2) for i in range(100))  # batch write
print(len(shelf2))  #output 100
print(shelf2[3]) #output 9
```

## Usage

Shelf class of package fastshelf is a dict-like wrapper, that can use any python objects as key and value.

Shelf handles the serialization and deserialization of python objects.

Shelf object initializes with arguments:
* backend: a dbm-like key-value storage database with "bytes" key and value, options include python3's own gnudb, PlyvelDB(via plyvel) and RocksDB(via lbry-rocksdb) or your own bytes-based key/value store
* serializer: a pickle-like serialization library, by default "pickle", which you can replace with dill/msgpack/bson/json
* track_value_changes: if True, enable **experimental** feature of tracking and writing back value object changes, to maintain similar new style object reference semantics with dict

You can use Shelf like a dict. In addition to standard dict interfaces, batch writes (Shelf.update) can significantly save time.
