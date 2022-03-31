
# speed testing code is borrowed from Tomotaka Ito (https://gist.github.com/tomotaka)

import os
import shutil

import shelf, dbm.gnu
from PlyvelDB import PlyvelDB
from RocksDB import RocksDB

class Dog:
    def __init__(self):
        self.number = 0

def test_trackchange():
    path = '/tmp/test.db'
    if os.path.exists(path):
        shutil.rmtree(path)
    dogshelf = shelf.Shelf(RocksDB(path),track_value_changes=True)
    for i in range(0,100):
        dog_i = Dog()
        dog_i.number = i
        dogshelf[i] = dog_i

    # create 100 dog references store in a list
    dogs = [dogshelf[i] for i in range(0,100)]
    # change dogs to have number 100-i
    for i in range(0,100):
        dogs[i].number = 100-i

    dogshelf.close()

    dogshelf = shelf.Shelf(RocksDB(path),track_value_changes=True)

    if list(dogshelf[i].number for i in range(0,100)) == list(100-i for i in range(0,100)):
        print('test_trackchange passed')
        return True


def test_speed():
    import time
    import hashlib
    import os
    from contextlib import contextmanager
    import shutil

    from sqlitedict import SqliteDict

    @contextmanager
    def measure(task):
        print(task)
        t1 = time.time()
        yield
        t2 = time.time()
        print('    -> %fsec' % (t2 - t1))

    def build_value(key:bytes):
        return hashlib.md5(key + 'hoge'.encode('UTF-8')).hexdigest().encode('UTF-8')

    def kv_gen(length):
        for i in range(length):
            key = str(i).encode('UTF-8')
            value = build_value(key)
            yield (key, value)

    def keys_gen(length):
        for i in range(length):
            key = str(i).encode('UTF-8')
            yield key

    def main():
        sqlitedict_f = '/tmp/benchmark_test.sqlite'
        sqlitedict_f2 = '/tmp/benchmark_test2.sqlite'
        sqlitedict_f3 = '/tmp/benchmark_test3.sqlite'
        sqlitedict_f4 = '/tmp/benchmark_test4.sqlite'
        rocksdb_dir = '/tmp/benchmark_test.rocks'
        plyveldb_dir = '/tmp/benchmark_test.plyvel'
        dbm_f = '/tmp/benchmark_shelve.db'

        # n = 1000000
        # n = 100000
        n = 60000

        def clean_up():
            if os.path.exists(rocksdb_dir):
                # os.removedirs(rocksdb_dir)
                shutil.rmtree(rocksdb_dir)
            if os.path.exists(plyveldb_dir):
                shutil.rmtree(plyveldb_dir)

            files = [
                sqlitedict_f, sqlitedict_f2, sqlitedict_f3,
                sqlitedict_f4, dbm_f
            ]
            for f in files:
                if os.path.exists(f):
                    os.remove(f)

        clean_up()

        with measure('Write: rocksdb N=%d' % n):
            my_db = shelf.Shelf(RocksDB(rocksdb_dir))
            for k, v in kv_gen(length=n):
                my_db[k] = v
            my_db.close()

        shutil.rmtree(rocksdb_dir)

        #with measure('Write: rocksdb sync mode N=%d' % n):
        #    my_db = shelf.Shelf(RocksDB(rocksdb_dir, sync=True))
        #    for k, v in kv_gen(length=n):
        #        my_db[k] = v
        #    my_db.close()
        #shutil.rmtree(rocksdb_dir)

        with measure('Write: rocksdb batch N=%d' % n):
            my_db = shelf.Shelf(RocksDB(rocksdb_dir, sync=True))
            my_db.update((k,v) for k, v in kv_gen(length=n))
            my_db.close()

        with measure('Read: rocksdb N=%d' % n):
            my_db = shelf.Shelf(RocksDB(rocksdb_dir))
            for key in keys_gen(length=n):
                v = my_db[key]
            # my_db.close()

        with measure('Write: dbm(gnudb) fast mode N=%d' % n):
            my_db = shelf.Shelf(dbm.gnu.open(dbm_f,'cf'))
            for k, v in kv_gen(length=n):
                my_db[k]=v
            my_db.close()

        os.remove(dbm_f)

        with measure('Write: dbm(gnudb) sync mode N=%d' % n):
            my_db = shelf.Shelf(dbm.gnu.open(dbm_f,'cs'))
            for k, v in kv_gen(length=n):
                my_db[k]=v
            my_db.close()

        with measure('Read: dbm(gnudb) N=%d' % n):
            my_db = shelf.Shelf(dbm.gnu.open(dbm_f,'r'))
            for key in keys_gen(length=n):
                v=my_db[key]
            my_db.close()

        with measure('Write: plyvel N=%d' % n):
            my_db = shelf.Shelf(PlyvelDB(plyveldb_dir))
            for k, v in kv_gen(length=n):
                my_db[k]=v
            my_db.close()

        import plyvel
        plyvel.destroy_db(plyveldb_dir)

        #with measure('Write: plyvel sync mode N=%d' % n):
        #    my_db = shelf.Shelf(PlyvelDB(plyveldb_dir,sync=True))
        #    for k, v in kv_gen(length=n):
        #        my_db[k]=v
        #    my_db.close()
        #plyvel.destroy_db(plyveldb_dir)

        with measure('Write: plyvel batch N=%d' % n):
            my_db = shelf.Shelf(PlyvelDB(plyveldb_dir,sync=True))
            my_db.update((k,v) for k, v in kv_gen(length=n))
            my_db.close()

        with measure('Read: plyvel N=%d' % n):
            my_db = shelf.Shelf(PlyvelDB(plyveldb_dir))
            for key in keys_gen(length=n):
                v = my_db[key]
            # my_db.close()



        with measure('Write: sqlitedb(commit once, no autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f3, autocommit=False)
            for k, v in kv_gen(length=n):
                mydict[k] = v
            mydict.commit()
            mydict.close()

        with measure('Write: sqlitedb(commit every time, no autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f4, autocommit=False)
            for k, v in kv_gen(length=n):
                mydict[k] = v
                mydict.commit()
            mydict.close()

        with measure('Write: sqlitedb(commit once, autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f2, autocommit=True)
            for k, v in kv_gen(length=n):
                mydict[k] = v
            mydict.commit()
            mydict.close()

        with measure('Write: sqlitedb(commit every time, autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f, autocommit=True)
            for k, v in kv_gen(length=n):
                mydict[k] = v
                mydict.commit()
            mydict.close()

        with measure('Read: sqlitedb(enabled autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f, autocommit=True)
            for key in keys_gen(length=n):
                mydict[key]
            mydict.close()

        with measure('Read: sqlitedb(no autocommit) N=%d' % n):
            mydict = SqliteDict(sqlitedict_f, autocommit=False)
            for key in keys_gen(length=n):
                mydict[key]
            # mydict.close()

        if my_db==mydict:
            print('data integrity test passed')

        mydict.close()
        my_db.close()

        clean_up()

    main()


if __name__ == '__main__':
    # test_data_integrity()

    test_speed()

    test_trackchange()
