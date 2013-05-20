import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    m_id = record[0]
    row = record[1]
    col = record[2]
    val = record[3]
            
    if (m_id == 'a'):
      for i in [0,1,2,3,4]:
        mr.emit_intermediate(tuple([row,i]), tuple(['a', col, val]))
    else:
      for i in [0,1,2,3,4]:
        mr.emit_intermediate(tuple([i, col]), tuple(['b', row, val]))
      

def reducer(key, list_of_values):

    def has_index(x, index):
        return x[1] == index
  
    def dot_product(list_of_values):
        sum = 0
        for i in [0,1,2,3,4]:
            results = []
            for k in list_of_values:
                if k[1] == i:
                    results.append(k[2])
            
            if (len(results) == 2):
                sum += (results[0] * results[1])
        return sum
    # key: tuple, (row,col) of final matrix
    # list_of_values, list of tuples, each tuple: (matrix_id, index, val)
    row = key[0]
    col = key[1]
    val = dot_product(list_of_values)
    mr.emit((row,col,val))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
