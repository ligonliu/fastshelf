from src.fastshelf import Shelf
import dbm.gnu
shelf1 = Shelf(dbm.gnu.open("/tmp/shelf1.gnudb",'cf'))

shelf1['a'] = 'A'
shelf1[(1,2,3)] = 'B'
shelf1[144] = {1,2,3,4,12}

print(dict(shelf1))
shelf1.close()

shelf1 = Shelf(dbm.gnu.open("/tmp/shelf1.gnudb",'cf'))
print(dict(shelf1))
print(shelf1[(1,2,3)])

