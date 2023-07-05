#!/bin/bash
aws elasticache describe-cache-clusters --query 'CacheClusters[*].ReplicationGroupId'|awk -F\" '{print $2}'|grep -v 'cluster'|grep -E 'scope-|aig-'|uniq|grep -v 'dandelion' > redis.list
aws elasticache describe-cache-clusters --output text|grep 'CACHECLUSTERS'|awk '{print $6}'|grep 'cluster'|grep -E 'scope-|aig-'|grep -v 'dandelion' >> redis.list
esEndpoint="log-es01:9200"
dateStamp=`date '+%Y%m%d%H%M'`
for i in `cat redis.list`;
do
	cd /data/scripts/scanRedis
	curl --user user:password  -XDELETE http://$esEndpoint/keysinfo-$i*
	if [[ $i =~ .*cluster.* ]];then
		redisEndpoint="$i.test.0001.apse1.cache.amazonaws.com:6379"
	else
		redisEndpoint="$i-ro.test.ng.0001.apse1.cache.amazonaws.com:6379"
	fi
	echo $redisEndpoint
	[ -f keys.txt ] && rm -f keys.txt
	[ -d files ] && rm -rf files
	mkdir files
	result=`cd /data/scripts/scanRedis && go run main.go $redisEndpoint|tail -1`
	ttlNum=`echo $result|awk '{print $1}'`
	bigNum=`echo $result|awk '{print $2}'`
	if [ $ttlNum's' == 's' ] || [ $bigNum's' == 's' ];then
		continue
	fi
	echo "$i:$ttlNum:$bigNum"
	if [ $ttlNum -gt 100 ] || [ $bigNum -gt 100 ];then
		content=$content$i"无TTL数:"$ttlNum"大键数:"$bigNum"\n"
	fi
	mv keys.txt files/ 
	cd files && split -l 500000 keys.txt && rm -f keys.txt
	for fileName in `ls`;do
		echo "$fileName"
		curl --user user:password -H 'Content-Type: application/x-ndjson'  -s -XPOST http://$esEndpoint/keysinfo-${i}_$dateStamp/keysinfo/_bulk --data-binary @/data/scripts/scanRedis/files/$fileName >/dev/null
	done
done
curl --insecure -X POST -H "Content-Type: application/json" -d '{"msg_type":"post","content":{"post":{"zh_cn":{"title":"Redis扫描结果","content":[[{"tag":"text","text":"'${content}'"}]]}}}}' https://open.feishu.cn/open-apis/bot/v2/hook/test
