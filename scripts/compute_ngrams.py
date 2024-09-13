from collections import Counter
from pathlib import Path

import conllu
import polars as pl


def get_ngrams(sent, n):
    # Split tokens that are bound by =
    tokens = list()
    pos = list()
    for token in sent:
        forms = token['form'].split('=') if token['form'] != '=' else token['form']
        tokens.extend(forms)
        pos.extend([token['upos']] * len(forms))
        
    # Ignore n-grams that are only punctuation (only _ upos)
    return [' '.join(tokens[i:i+n])
            for i in range(len(tokens)-n+1)
            if any(tag != '_' for tag in pos[i:i+n])]


max_n = 5
counters = [Counter() for _ in range(max_n)]

# corpus_path = Path('data/brwac.conll')
corpus_path = Path('data/brwac.conll')
with corpus_path.open('r', encoding='utf-8') as f:
    for sent in conllu.parse_incr(f):
        for i in range(max_n):
            counters[i].update(get_ngrams(sent, i+1))

# Convert counts to polars DataFrames
dfs = [(pl.DataFrame(counter)
        .unpivot(variable_name='ngram', value_name='freq')
        .sort(by='freq', descending=True))
       for counter in counters]

# Save results
for i in range(max_n):
    filepath = Path('results') / f'{i+1}gram.csv'
    filepath.parent.mkdir(exist_ok=True, parents=True)
    df = dfs[i]
    df.write_csv(filepath)
