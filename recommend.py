from pyspark import SparkContext, SparkConf
conf = SparkConf()
sc = SparkContext(conf=conf)

import sys
import argparse

parser = argparse.ArgumentParser(description='Find twitter users similar to provided user, based on tweet content.')
parser.add_argument('-user', type=str, help='The name of the user.', required=True)
parser.add_argument('-k', type=int, help='The number of recommended users to be returned.', required=True)
parser.add_argument('-file', type=str, help='Path to the file containing the data set.', required=True)
parser.add_argument('-output', type=str, help='Path to the output file where the results will be written.',
                    required=True)

args = parser.parse_args(sys.argv[1:])


tweets = sc.textFile(args.file).map(lambda x: x.split('\t')).map(lambda x: (x[0], x[1].split(' ')))
print(tweets.first())


