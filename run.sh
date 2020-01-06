spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-memory 2g \
--executor-cores 1 \
--num-executors 10 \
--driver-memory 2g \
--name "example_data" \
main.py
