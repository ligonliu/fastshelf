import collections.abc, rocksdb, shutil


class RocksDB(collections.abc.MutableMapping):
    """
    RocksDB map interface, both key and value are bytes object
    """
    def __init__(self, db_path:str, sync=False):
        self.db_path = db_path
        self.sync = sync
        self._db: rocksdb.DB = rocksdb.DB(self.db_path, rocksdb.Options(create_if_missing=True))

    def get(self, key:bytes, default=None):
        return self._db.get(key,default)

    def __getitem__(self, key:bytes):
        return self._db.get(key)

    def __setitem__(self, key:bytes, val:bytes):
        return self._db.put(key, val, sync=self.sync)

    def __delitem__(self, key:bytes):
        self._db.delete(key,sync=self.sync)

    def keys(self):
        it = self._db.iterkeys()
        it.seek_to_first()
        return it

    def items(self):
        it = self._db.iteritems()
        it.seek_to_first()
        return it

    def __contains__(self, key):
        return self._db.get(key) is not None

    __iter__ = keys

    def __len__(self):
        count = 0
        for _ in self.keys():
            count += 1
        return count

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def update(self, source):

        batch = rocksdb.WriteBatch()
        for k,v in (source.items if 'items' in dir(source) else source):
            # dict-like source vs iterable of (k,v) pairs
            batch.put(k,v)

        self._db.write(batch, sync=self.sync)

    def clear(self) -> None:
        # close, destroy, reopen
        self._db.close()
        shutil.rmtree(self.db_path)
        self._db = rocksdb.DB(self.db_path, rocksdb.Options(create_if_missing=True))