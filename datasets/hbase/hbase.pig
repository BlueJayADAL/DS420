REGISTER /home/hadoop/hbase-2.2.6/lib/zookeeper-3.4.10.jar;
REGISTER /home/hadoop/hbase-2.2.6/lib/hbase-client-2.2.6.jar;

users = LOAD '/user/lip/ds420/u.user' USING PigStorage('|')
AS (userID:int,
	age:int,
	gender:chararray,
	occupation:chararray,
	zip:int);

STORE users INTO 'hbase://users_lip'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage (
'userinfo:age userinfo:gender userinfo:occupation userinfo:zip');
