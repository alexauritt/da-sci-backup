import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    mr.emit_intermediate(record[1], record)

def reducer(key, list_of_values):
  # extract order info from list of values
  # iterate through remaining values and pair with order_info
  
  def is_order_info(x):
    if x[0] == 'order':
      return True
    return False
  
  def is_line_item_info(x):
    if x[0] == 'line_item':
      return True
    return False
    
  order_info = filter(is_order_info, list_of_values)
  line_items = filter(is_line_item_info, list_of_values)
  # print len(line_items)
  # print
  for m in line_items:
    mr.emit((order_info[0] + m))


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
