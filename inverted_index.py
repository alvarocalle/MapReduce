"""
  Problem 1: Create an Inverted index
"""
import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key  : document ID
    # value: document contents
    key   = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)
#    for key in mr.intermediate:
#        print key, mr.intermediate[key]

def reducer(key, list_of_values):
    # key: word
    # value: list of docs
    list_of_values = list(set(list_of_values))
    mr.emit((key,list_of_values))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
