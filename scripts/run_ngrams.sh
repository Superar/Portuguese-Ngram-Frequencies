# Split NILC corpora to avoid memory overflow
for corpus in ../../Corpora/nilc_embedding_pub_corpus/*.txt; do
    if [ $(wc -l < $corpus) -gt 250000 ]; then
        split -l 250000 --additional-suffix=.txt -d $corpus ${corpus%.txt}
        rm $corpus
    fi
done

for corpus in ../../Corpora/nilc_embedding_pub_corpus/*.txt; do
    echo "$(basename $corpus)"
    pipenv run python scripts/compute_ngrams.py --corpus $corpus
done
