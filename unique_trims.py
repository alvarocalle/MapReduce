"""
Problem 5: unique trimmed nucleotide strings Set of key-value pairs
(id, string of nucleotides), e.g., GCTTCCGAAATGCTCGAA...  Write a
MapReduce query to remove the last 10 characters from each string of
nucleotides, then remove any duplicates generated.
"""
import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key  : document ID
    # value: document contents
    key = record[0]
    dna = record[1]
    trimmed = dna[:len(dna)-10]
    mr.emit_intermediate(trimmed, 1)

def reducer(key, list_of_values):
    # key: dna string
    # value: list of docs
    mr.emit(key)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
