##########代码
read2text.py是代码，test.sh是运行脚本，把这两个文件放到assignment里面

##########环境
1.从docker中提取
	docker pull starightedge7/wdps_2126:v1
	sudo docker run --privileged -ti -v ~/master/wdps/assignment:/app/assignment -p 9200:9200 starightedge7/wdps_2126:v1

2.自己根据老师的imgae创建
	sudo docker run --privileged -ti -v ~/master/wdps/assignment:/app/assignment -p 9200:9200 karmaresearch/wdps_assignment
	在container里面
	pip3 install --upgrade pip
	pip3 install pyspark
	pip3 install elasticsearch
	pip3 install spacy
	pip3 install requests
	pip3 install nltk
	python3 -m spacy download en_core_web_lg
	python3 -m spacy download en_core_web_sm

##########运行
1. sudo sysctl -w vm.max_map_count=262144
2. sh start_elasticsearch_server.sh
3. sh test.sh
	

# wdps_2126
