bench=bench.out
#echo 'engine,event_id,action,data_size (bytes),duration (ns)' > ${bench}
python evaluation/kafka/cli.py kafka -e 9170b3b1-12a0-44c3-845e-4d0aa6e52323 -n 10 -l INFO -f ${bench}

#bedrock ofi+tcp -c ./evaluation/mofka/src/config.json &
#python evaluation/kafka/cli.py mofka -e 9170b3b1-12a0-44c3-845e-4d0aa6e52323 -n 10 -l INFO -f ${bench}
#pkill -f bedrock