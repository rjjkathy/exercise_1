Steps to run:

	1. start hadoop in UCB complete AMI
	2. start postgres
	3. run load_data_lake.sh which gets the data, rename and removes head, then put into hdfs
	4. tables are created by py scripts under transforming
	5. for answers to questions, can directly run spark-submit /path/to/investigation/scripts.py which include creating tables and querying 
	6. .txt files are verbal answers
	