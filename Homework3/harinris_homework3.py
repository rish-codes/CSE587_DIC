from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

# Creating a local StreamingContext with two execution threads and batch interval of 1 second.
conf = SparkConf().setAppName("WordCoOccurrence").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

# Creating a DStream that represents streaming data from a TCP source
lines = ssc.socketTextStream("localhost", 9999)

# Splitting each line into substrings by periods, then splitting each substring on spaces.
def process_line(line):
    substrings = line.split(".")
    return [substring.strip().split() for substring in substrings]

# Cleaning substrings by removing non-alphanumeric characters and converting to lowercase.
def clean_substring(substring):
    return [word.lower().replace(r'\W', '') for word in substring if word]

# Forming bigrams.
def find_bigrams(words_list):
    return [(words_list[i], words_list[i+1]) for i in range(len(words_list) - 1)]

# Grouping the bigrams and counting their frequencies
word_counts = lines.flatMap(process_line) \
    .map(clean_substring) \
    .flatMap(find_bigrams) \
    .map(lambda bigram: (bigram, 1)) \
    .reduceByKey(lambda x, y: x + y)

# Printing word co-occurrence counts in a readable format.
def print_word_counts(word_count):
    bigram, count = word_count
    print(f"{bigram}: {count}")

word_counts.foreachRDD(lambda rdd: rdd.foreach(print_word_counts))

ssc.start()
ssc.awaitTermination()
