"""
  Problem 2: Implement a relational join as a MapReduce query

  SELECT * 
  FROM Orders, LineItem 
  WHERE Order.order_id = LineItem.order_id
"""
import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key  : order ID
    # value: everything
    key   = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: order ID
    orderID    = list_of_values[0]
    line_items = list_of_values[1:]

    for l in line_items:
        mr.emit(orderID + l)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
