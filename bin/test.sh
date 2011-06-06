
erlc *.erl

erl -noshell -eval "eunit:test(all_tests, [verbose])" -s init stop
