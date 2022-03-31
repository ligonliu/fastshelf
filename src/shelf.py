import collections.abc, pickle, weakref, sys
from bidict import bidict


class Shelf(collections.abc.MutableMapping):

    def __init__(self, backend, serializer=pickle, track_value_changes=False):
        self.backend = backend
        self.serializer = serializer
        self.track_value_changes = track_value_changes
        if track_value_changes:
            self.type_map = bidict()
            self.weakref_value_to_key = weakref.WeakKeyDictionary()
            self.key_to_weakref_value = weakref.WeakValueDictionary()

    def __iter__(self):
        for k in self.backend.keys():
            yield self.serializer.loads(k)

    def __len__(self):
        return len(self.backend)

    def __contains__(self, key):
        return self.serializer.dumps(key) in self.backend

    def get(self, key, default=None):
        key_bytes = self.serializer.dumps(key)
        value_bytes = self.backend.get(key_bytes)
        if value_bytes is None:
            return default
        value = self.serializer.loads(value_bytes)

        if self.track_value_changes:
            self._registerTracker(key, value)

        return value

    def __getitem__(self, key):
        key_bytes = self.serializer.dumps(key)

        value_bytes = self.backend[key_bytes]
        if value_bytes is None:
            raise KeyError(key)

        value = self.serializer.loads(value_bytes)

        if self.track_value_changes:
            self._registerTracker(key, value)

        return value

    def _registerTracker(self, key, value):
        if value.__class__ not in self.type_map:
            new_class = self._syncAtFinalizeClassFactory(value)
            self.type_map[value.__class__] = new_class
        value.__class__ = self.type_map[value.__class__]
        self.weakref_value_to_key[value] = key
        self.key_to_weakref_value[key] = value

    def _deregisterTracker(self, key, value):
        if value in self.weakref_value_to_key:
            del self.weakref_value_to_key[value]
        if key in self.key_to_weakref_value:
            del self.key_to_weakref_value[key]
        if value.__class__ in self.type_map.inverse:
            value.__class__ = self.type_map.inverse[value.__class__]

        # check if value has changed, if it has changed, write it to backend
        try:
            key_bytes = self.serializer.dumps(key)
            value_bytes_in_db = self.backend.get(key_bytes)
            if value_bytes_in_db is not None:
                value_bytes = self.serializer.dumps(value)
                if value_bytes != value_bytes_in_db:
                    self.backend[key_bytes] = value_bytes
        except Exception as e:
            print('Exception in deregisterTracker', key, e, file=sys.stderr)


    def __setitem__(self, key, value):
        key_bytes = self.serializer.dumps(key)
        value_bytes = self.serializer.dumps(value)
        self.backend[key_bytes] = value_bytes

    def __delitem__(self, key):
        key_bytes = self.serializer.dumps(key)
        del self.backend[key_bytes]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.backend is None:
            return
        try:
            if self.track_value_changes:
                # write all to db
                for k,v in list(self.key_to_weakref_value.items()):
                    self._deregisterTracker(k, v)
            try:
                self.backend.close()
            except AttributeError:
                pass
        finally:
            # Catch errors that may happen when close is called from __del__
            # because CPython is in interpreter shutdown.
            self.backend = None

    def __del__(self):
        if not hasattr(self, 'serializer'):
            # __init__ didn't succeed, so don't bother closing
            # see http://bugs.python.org/issue1339007 for details
            return

        self.close()

    def update(self, source, **kwargs):

        sour = (source.items if 'items' in dir(source) else source)

        if hasattr(self.backend, 'update'):
            self.backend.update(((self.serializer.dumps(k), self.serializer.dumps(v)) for k, v in sour))
            self.backend.update(((self.serializer.dumps(k), self.serializer.dumps(v)) for k, v in kwargs.items()))
        else:
            for k, v in sour:
                self.backend[self.serializer.dumps(k)] = self.serializer.dumps(v)
            for k, v in kwargs.items():
                self.backend[self.serializer.dumps(k)] = self.serializer.dumps(v)

    def _syncAtFinalizeClassFactory(self, obj):
        original_class = obj.__class__

        def __del__(this):
            # update the value in the DB
            # read first, then write
            try:
                key = self.weakref_value_to_key.get(this)
                if key is not None:
                    # temporarily set class back before storing the class
                    current_class = this.__class__
                    this.__class__ = original_class
                    self[key] = this
                    this.__class__ = current_class

                    self._deregisterTracker(key, this)
            except Exception as e:
                print('Exception in finalizer for', original_class, this, e, file=sys.stderr)

            try:
                original_class.__del__(this)
            except AttributeError:
                pass

        new_class = type(obj.__class__.__name__, (obj.__class__,), {"__del__": __del__})
        return new_class

