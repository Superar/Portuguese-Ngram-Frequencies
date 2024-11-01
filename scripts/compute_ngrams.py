from argparse import ArgumentParser
from operator import add
from pathlib import Path

import polars as pl
from pyspark.sql import SparkSession

parser = ArgumentParser()
parser.add_argument('-n', required=True, type=int,
                    help='Number of tokens in the n-gram')
args = parser.parse_args()


def get_ngrams(line, n):
    line = line.strip()
    # Split tokens that are bound by =
    tokens, tags = line.split('\t')
    new_tokens, new_tags = list(), list()
    for token, tag in zip(tokens.split(), tags.split()):
        forms = token.split('=') if token != '=' else token
        new_tokens.extend(forms)
        new_tags.extend([tag] * len(forms))
    # Ignore n-grams that are only punctuation (only _ upos)
    return [' '.join(new_tokens[i:i+n])
            for i in range(len(tokens)-n+1)
            if any(tag != '_' for tag in new_tags[i:i+n])]


num_examples_to_save = [10000, 5000, 3000, 1000, 1000]

corpus_path = Path('data/brwac_awk.txt')
spark = (SparkSession.builder.appName('PortugueseNgramFrequency').getOrCreate())
corpus_rdd = spark.sparkContext.textFile(str(corpus_path))

ngram_rdd = corpus_rdd.flatMap(lambda lines: get_ngrams(lines, args.n))
ngram_pairs = ngram_rdd.map(lambda ngram: (ngram.lower(), 1))
ngram_counts = ngram_pairs.reduceByKey(add)
sorted_ngram_counts = ngram_counts.sortBy(lambda x: x[1], ascending=False)

df = pl.DataFrame(sorted_ngram_counts.take(num_examples_to_save[args.n-1]),
                  strict=False, orient='row',
                  schema=[('ngram', pl.Utf8), ('freq', pl.Int32)])

# Save the n-gram frequencies to a CSV file
filepath = Path('results') / f'{args.n}gram.csv'
filepath.parent.mkdir(exist_ok=True, parents=True)
df.write_csv(filepath)
