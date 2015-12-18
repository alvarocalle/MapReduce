"""
Problem 3: simple social network. 
MapReduce algorithm to count the number of friends for each person.
"""
import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key  : person
    # value: number of friends
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: person
    # value: list of occurrence counts
    num_friends = sum(list_of_values)
    mr.emit((key, num_friends))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
