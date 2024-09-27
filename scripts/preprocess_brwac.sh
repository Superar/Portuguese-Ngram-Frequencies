# perl -i -ne 'print unless /^# /' data/brwac.conll
# perl -i -ple 'print "" if $. > 1 && /^1\t/' data/brwac.conll

awk '
BEGIN { sentence_tokens=""; sentence_tags=""; }
/^$/ {
    if (sentence_tokens != "" && sentence_tags != "") {
        print sentence_tokens "\t" sentence_tags;
        sentence_tokens="";
        sentence_tags="";
    }
}
!/^#/ && NF > 0 {
    token=$2;
    tag=$4;
    sentence_tokens = (sentence_tokens == "" ? token : sentence_tokens " " token);
    sentence_tags = (sentence_tags == "" ? tag : sentence_tags " " tag);
}
END {
    if (sentence_tokens != "" && sentence_tags != "") {
        print sentence_tokens "\t" sentence_tags;
    }
}
' "data/brwac.conll" > "data/brwac_awk.txt"
