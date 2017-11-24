#!/bin/bash
###############################################################################################注释的代码
:<<EOF
运行参数: 
			1.      ShardSourceTB   sharding的表名
			2.      basesize                hdfs文件大小基准(默认是10M,即10485760)
			3.      basepath                hdfs根路径（默认是'hdfs://ns/user/hive/warehouse'）
			4.      CurrentDate             分区日期(默认是当天)
功能描述   ：校验取sharding的n个表是否有异常
（校验点：判当前分区的文件大小是否大于10M,即10485760
  只判断分区是否存在不严谨，如果没取到数据也会生成分区！
）
注意     ： 
修改历史 ： 修改人     修改时间      主要改动说明
1               jblu            2015-12-19    1、create
2.              jblu            2015-12-26        1.增加计算表的行数（晚上测试时发现文件3G左右，统计行数耗时超30s，因此不统计了）
                                                  2.ShardSourceTB 转换为小写
                                                  3.fileSize改为精确到M
                                                  4.fileSize改为精确到K(由于RoomQuantitiy、RoomInfo_ExtraInfo表增量过小，低于1M,导致无法判断)
3.              jblu            2016-09-27        1.去除rowcounts提醒;
4.              jblu            2016-09-28        1.升级出错提醒，便于仅重跑错误的shard表;
                                                  2.配置的文件名尾部增加随机数，防止读错文件；
5.              jblu            2017-01-16        1、带上二级分区进行校验；                                                                   
6.              jblu            2017-01-19        1、插入ShardTB表中的TargetTB字段必须使用${ShardSourceTB},否则无法查询出报错的ShardIDS；
7.              j_le            2017-04-17        1.将配置表修改到sql server，直接从sql server读取配置信息
												  2.增加变量BeginTime，在校验判定的时候增加条件CheckTime>=${BeginTime},填补之前一天多次跑数据，会查出脏数据的漏洞
8               j_le            2017-09-06        1.由于之前直接用filesize和basesize比较大小，所以取整了，导致数据判断不准，现直接比较浮点数
EOF
################################################################################################注释的代码


#Sharding的表名（必须使用小写）
#ShardSourceTB=$1     #modify 2.2
declare -l ShardSourceTB=$1

#文件大小标准
basesize=$2

#hdfs
basepath=$3

#分区日期
CurrentDate=$4


echo '==================BEGIN DATACHECK============================'
BeginTime=$(date +"%Y-%m-%d %H:%M:%S")
#配置文件名(防止多个进程读写同一个文件名)
#filename=shardingbase${ShardSourceTB}check.info
filename=shardingbase${ShardSourceTB}check${RANDOM}.info                #modify 4.2

#hive -e "set hive.cli.print.header=false;      
#select lower(TargetDB) as TargetDB,lower(TargetTB) as TargetTB,ShardID 
#from dw_htlmaindb.vShardingBaseInfo 
#where upper(ShardingTB)=upper('${ShardSourceTB}') and isvalid='T';">./${filename}        #modify 7.1

sh sql_run.sh -t sqlserver -h XXXXXXXXXXX -P XXXXXXXXX -u XXXXXXXXXXXXX -p XXXXXXXXXXXXXXXXX -e "
								SELECT LOWER(TargetDB) AS TargetDB,LOWER(TargetTB) AS TargetTB,ShardID
								FROM [PublicDB].[dbo].[vShardingBaseInfo]
								WHERE ShardingTB = '${ShardSourceTB}' AND IsValid='T'">./${filename}

#去掉第一行字段名
sed -i '1d' ${filename}

#cat shardingbase.info

#从ShardingBaseInfo中提取2个字段(一行总计2个字段)
colnum=3

#sharding配置信息放入数组
arr=($(cat ${filename}))
length=${#arr[@]}
#echo $length
ErrorMsg=''

for((i=0;i<length;i=$(($i+${colnum})) ));do

  TargetDB=${arr[i+0]}
  TargetTB=${arr[i+1]}
  ShardID=${arr[i+2]}
  
  hdfsPath=${basepath}/${TargetDB}.db/${TargetTB}/d=${CurrentDate}/shardid=${ShardID}


   #取分区文件大小
   fileSize=`hadoop fs -count ${hdfsPath} | awk -F ' ' 'BEGIN {size=0;} {size=size+$3;} END{print int(size)/1024}'`
   #fileSize=`expr ${fileSize} / 1024 / 1024`   #modify 2.3
   #fileSize=`expr ${fileSize} / 1024 `  #modify 3.1
   #取分区的文件行数（即表行数）
   #RowCounts=`hadoop fs -cat ${hdfsPath}/* | wc -l `
   #RowCounts=0
   
   #取分区是否存在（存在即表示datax写分区数据成功了,不严谨，如果没取到数据也会是空）
   #hadoop fs -test -d ${hdfsPath}
   #ret=`echo $?`
   
   
        echo "=========CHECK ${TargetTB} ShardID:${ShardID} filesize:${fileSize} ============"
   
   checkFileSize=`echo "$fileSize > $basesize" | bc`
   if [ ${checkFileSize} -eq 1 ]
   then
                IsSuccess=1
   else
                IsSuccess=0
        ErrorMsg=${ErrorMsg}${TargetTB}"分区[${CurrentDate}/${ShardID}]文件FileSize过小(${fileSize})；"
   fi
   
   
	#query="SELECT '${TargetDB}' AS TargetDB,'${TargetTB}' AS TargetTB,${fileSize} as filesize,${IsSuccess} AS IsSuccess,${RowCounts} as RowCounts,${ShardID} as ShardID "
	#query="SELECT '${TargetDB}' AS TargetDB,'${ShardSourceTB}' AS TargetTB,${fileSize} as filesize,${IsSuccess} AS IsSuccess,${RowCounts} as RowCounts,${ShardID} as ShardID "  #modify 6.
	query="SELECT '${TargetDB}' AS TargetDB,'${ShardSourceTB}' AS TargetTB,${fileSize} as filesize,${IsSuccess} AS IsSuccess,${ShardID} AS ShardID,GETDATE() AS CheckTime"
	query_datacheck=${query_datacheck}${query}' union all '

done


query_datacheck=${query_datacheck%union all*}   #去除最后一个字符“union all”
#echo ${query_datacheck}
#query_datacheck="use dw_htlmaindb;
#				 INSERT INTO TABLE dw_htlmaindb.ShardTBCheck partition (d='${CurrentDate}')
#				 SELECT TargetDB,TargetTB,filesize,IsSuccess,ShardID
#                 FROM(
 #                  ${query_datacheck}
#                 )M;              
#"
sh sql_run.sh -t sqlserver -h XXXXXXXXXXXXXX -P XXX -u XXXXXXXXXXX -p XXXXXXXXXXXXXXX -e "USE PublicDB;
					INSERT INTO ShardDataCheckLog 
					SELECT TargetDB,TargetTB,filesize,IsSuccess,ShardID,CheckTime
					FROM(
						${query_datacheck}
					)M;"
#echo ${query_datacheck}

#hive -e "${query_datacheck}"

#判断如果ErrorMsg不为空，则报错退出
if [ ${ErrorMsg} ]
then

        #query_ErrorShardID="
        #set hive.cli.print.header=false;
        #SELECT
        #CONCAT_WS(',',collect_set(cast(ShardID as string))) as shardids
        #FROM dw_htlmaindb.ShardTBCheck a
        #WHERE a.d='${CurrentDate}' AND lower(a.tb) like '%${ShardSourceTB}%' and IsSuccess=0
        #"
        #Error_ShardIDS=`hive -e "${query_ErrorShardID}"`
		
		Error_ShardIDSFileName=Error_ShardIDS${RANDOM}.info
		sh sql_run.sh -t sqlserver -h XXXXXXXXXXXXXXX -P XXXXXX -u XXXXXXXXXXXX -p XXXXXXXXXXXXXXXX -e "
					SELECT ShardID = ( STUFF(( SELECT    ',' + CAST (ShardID as varchar(5))
											   FROM      PublicDB..ShardDataCheckLog (NOLOCK)
											   WHERE     TargetTB = A.TargetTB
														 AND IsSuccess=0 
														 AND CheckTime >= '${BeginTime}' 
														 AND CheckTime <= GETDATE()
														 AND TargetTB = '${ShardSourceTB}'
											FOR XML PATH('')), 1, 1, '') )
					FROM PublicDB..ShardDataCheckLog (NOLOCK) AS A
					WHERE IsSuccess=0 
						  AND CheckTime >= '${BeginTime}' 
						  AND CheckTime <= GETDATE()
						  AND TargetTB = '${ShardSourceTB}'
					GROUP BY A.TargetTB
		">./${Error_ShardIDSFileName}
		sed -i '1d' ${Error_ShardIDSFileName}
		Error_ShardIDS=($(cat ${Error_ShardIDSFileName}))
		
        echo "=========Error:Sharding导入数据出错，可选择在“ErrorRetryCondition”变量中粘贴下述返回值，避免重跑已正确的数据：==============="

        echo "=========可尝试在脚本的“ErrorRetryCondition”中粘贴如下条件：AND ShardID IN("${Error_ShardIDS}")"

    #echo "SELECT db,tb,filesize FROM dw_htlmaindb.ShardTBCheck WHERE D='${CurrentDate}' AND lower(tb) like '%${ShardSourceTB}%' and IsSuccess=0 "
        #modify 4.
        echo "==如下SQL可查询Error的表:SELECT b.TargetDB,b.TargetTB,a.filesize,b.ShardID,b.SourceDB,b.SourceTB FROM PublicDB..ShardDataCheckLog (NOLOCK) a INNER JOIN [PublicDB].[dbo].[vShardingBaseInfo] b ON (a.TargetTB)=(b.ShardingTB) and a.shardid=b.shardid WHERE a.CheckTime >= '${BeginTime}' AND a.CheckTime <= GETDATE() AND a.TargetTB = '${ShardSourceTB}' and IsSuccess=0"
    echo "=========Error:Sharding导入数据出错，详细信息如下：==============="
    echo ${ErrorMsg}
    exit 1
else
		echo '==================END DATACHECK============================'
fi

echo "====================Sharding导入数据成功！==============="