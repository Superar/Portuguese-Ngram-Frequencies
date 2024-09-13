perl -i -ne 'print unless /^# /' data/brwac.conll
perl -i -ple 'print "" if $. > 1 && /^1\t/' data/brwac.conll
