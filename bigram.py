from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], use_unicode=False)
    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split("."))

    #Your code goes here
    bigrams = sentences.map(lambda x: x.split()) \
    .flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])

    freqnt = bigrams.reduceByKey(lambda x,y: x+y) \
    .map(lambda x:(x[1],x[0])) \
    .sortByKey(False)

    freqnthundred = sc.parallelize(freqnt.take(100))
    freqnthundred.saveAsTextFile("freqnthundredbigrams.out")

    sc.stop()
