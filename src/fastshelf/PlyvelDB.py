import collections.abc, plyvel

class PlyvelDB(collections.abc.MutableMapping):
    """
    LevelDB map interface, both key and value are bytes object
    """
    def __init__(self, db_path:str, sync=False):
        self.db_path = db_path
        self.sync = sync
        self._db: plyvel.DB = plyvel.DB(self.db_path, create_if_missing=True)

    def get(self, key:bytes, default=None):
        return self._db.get(key,default)

    def __getitem__(self, key:bytes):
        return self._db.get(key)

    def __setitem__(self, key:bytes, val:bytes):
        return self._db.put(key, val, sync=self.sync)

    def __delitem__(self, key:bytes):
        self._db.delete(key,sync=self.sync)

    def keys(self):
        for k, _ in self._db:
            yield k

    def items(self):
        for k, v in self._db:
            yield k, v

    def __contains__(self, key):
        return self._db.get(key) is not None

    def iterkeys(self):
        for k, _ in self._db:
            yield k
    __iter__ = iterkeys

    def __len__(self):
        count = 0
        for _, _ in self._db:
            count += 1
        return count

    def close(self):
        self._db.close()

    __del__ = close

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def update(self, source):

        with self._db.write_batch(transaction=True,sync=self.sync) as b:
            for k,v in (source.items if 'items' in dir(source) else source):
                # dict-like source vs iterable of (k,v) pairs
                b.put(k,v)

    def clear(self) -> None:
        # close, destroy, reopen
        self._db.close()
        plyvel.destroy_db(self.db_path)
        self._db = plyvel.DB(self.db_path, create_if_missing=True)