from argparse import ArgumentParser
from collections import Counter
from pathlib import Path

import polars as pl
import spacy


def get_ngrams(doc, n):
    return [doc[i:i+n].text for i in range(len(doc)-n+1)
            if not {'SPACE', 'PUNCT'} & {doc[i+j].pos_ for j in range(n)}]

parser = ArgumentParser(description='Get n-gram counts from a text file.')
parser.add_argument('--corpus', '-c',
                    help='Raw text file from which to count the n-grams.' +
                    'Must have one sentence or text per line.',
                    required=True, type=Path)
args = parser.parse_args()

spacy.require_gpu()
nlp = spacy.load('pt_core_news_lg')

max_n = 5
counters = [Counter() for _ in range(max_n)]

# Use spacy to get ngrams and counts
with args.corpus.open(encoding='utf-8') as f:
    docs = nlp.pipe(f)
    for doc in docs:
        for i in range(max_n):
            counters[i].update(get_ngrams(doc, i+1))

# Convert counts to polars DataFrames
dfs = [(pl.DataFrame(counter)
        .unpivot(variable_name='ngram', value_name='freq')
        .sort(by='freq', descending=True))
       for counter in counters]

# Save partial counts
for i in range(max_n):
    filepath = Path('results') / f'{i+1}gram' / args.corpus.stem
    filepath = filepath.with_suffix('.csv')
    filepath.parent.mkdir(exist_ok=True, parents=True)

    df = dfs[i]
    df.write_csv(filepath)

# # Merge results with counts from previous corpora
# for i in range(max_n):
#     filepath = Path('data') / f'{i+1}gram.csv'
#     df = dfs[i]
#     if filepath.exists():
#         file_df = pl.read_csv(filepath)
#         df = (pl.concat([file_df, df])
#               .group_by(pl.col('ngram'))
#               .agg(pl.sum('freq'))
#               .sort(by=pl.col('freq'), descending=True))
#     df.write_csv(filepath)
