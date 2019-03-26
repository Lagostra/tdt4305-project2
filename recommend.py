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


# Collect all tweets
tweets = sc.textFile(args.file).map(lambda x: x.split('\t')) 

# Map to key-value pairs with (user, word) as key and frequency as value
word_counts = tweets.flatMapValues(lambda x: x.split()) \
				.map(lambda x: ((x[0], x[1]), 1)) \
				.reduceByKey(lambda x, y: x + y)

# Get word-frequency pairs of the query user
query_counts = word_counts.filter(lambda x: x[0][0] == args.user).map(lambda x: (x[0][1], x[1]))

# Remove query user, and map to word as key
word_counts = word_counts.filter(lambda x: x[0][0] != args.user).map(lambda x: (x[0][1], (x[0][0], x[1])))

# Join query and haystack, and take minimum of all frequencies + make user key
frequency_scores = word_counts.join(query_counts).map(lambda x: (x[1][0][0], min(x[1][0][1], x[1][1])))

# Sum all frequency scores to get the similarity, and order by similarity (and username)
similarities = frequency_scores.reduceByKey(lambda x, y: x + y).sortBy(lambda x: (-x[1], x[0]))

top_k = similarities.zipWithIndex().filter(lambda x: x[1] < (args.k)).map(lambda x: x[0])

top_k.map(lambda x: x[0] + '\t' + str(x[1])).coalesce(1).saveAsTextFile(args.output)
