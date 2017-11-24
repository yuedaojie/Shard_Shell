#!/bin/bash
###############################################################################################注释的代码
:<<EOF


参数:
				1.batchNum		取数批次定义，每批datax任务保持在30个以下，过多并发，容易出系统问题
				2.ShardSourceTB		Sharding的表名，一个Sharding job的唯一标识
				3.SelectCOLS		生产上源表的字段列表
				4.SQLSERVERorMYSQL		生产上数据源类型：SQLSERVER/MYSQL
				5.Condition		取数条件，初始化时的条件：Condition="1=1"，正常流程的条件（增量）：Condition="DataChange_LastTime>='${before3_date}'"
				6.ErrorRetryCondition		错误重跑时可选择使用的条件，在输出日志中找到发生错误的ShardID，填入下面的条件中；ErrorRetryCondition=" AND ShardID IN()"
功能描述  :  
注意     ：
输入表   : 源头N个sharding表
输出表   ：N个对应的sharding目的表ODS_HTL_htlroompricerequestordersharddb.contractauditfinishlist_base
修改历史 ： 修改人     修改时间      主要改动说明
1         j_le     	2017-04-18   1、create
2         j_le      2017-05-05   1、将内嵌的shell，直接解开写到当前shell中

EOF
################################################################################################注释的代码

#########################基础参数》》
#取数批次定义，每批datax任务保持在30个以下，过多并发，容易出系统问题
batchNum=$1
#Sharding的表名
ShardSourceTB=$2
#源表的字段列表
SelectCOLS=$3
#数据源类型：SQLSERVER/MYSQL
SQLSERVERorMYSQL=$4
#取数条件
Condition=$5
#错误重跑时可选择使用的条件
ErrorRetryCondition=$6
#########################《《基础参数

v_schema='..'
v_nolock=' with (nolock)'

if [ ${SQLSERVERorMYSQL} = 'MYSQL' ]
then
	v_schema='.'
    v_nolock=''
fi

#配置文件名(文件名中增加随机数，防止多个进程读写同一个文件名)
filename=shardingbase${ShardSourceTB}${RANDOM}.info

getshardinfo_query="SELECT ShardID,SourceDB,SourceTB,TargetDB,TargetTB
					FROM [PublicDB].[dbo].[vShardingBaseInfo]
					WHERE ShardingTB = '${ShardSourceTB}' AND IsValid='T' "
getshardinfo_query=${getshardinfo_query}${ErrorRetryCondition}

sh sql_run.sh -t sqlserver -h XXXXXXXXXX -P xxxx -u XXXXXXXXXXXX -p XXXXXXXXXXXXXX -e "${getshardinfo_query}">./${filename}

#去掉第一行字段名
sed -i '1d' ${filename}

#sharding配置信息放入数组
arr=($(cat ${filename}))
length=${#arr[@]}
echo "length="$length

#从ShardingBaseInfo中提取5个字段(一行总计5个字段)
colnum=5

#取数批次控制
intervalNum=$((length/batchNum+1))

i=0
for((j=0;j<${batchNum};j++));
do
	loopvalue=$i	#关键参数
	m=$[(j+1)*intervalNum]

	if [ $m -ge $length ]
	then
		echo "最后一次循环"
		m=$length
	fi

	for((i=$loopvalue;i<$m;i=$[i+colnum]));
	do
		ShardID=${arr[i]}
		SourceDB=${arr[i+1]}
		SourceTB=${arr[i+2]}
		TargetDB=${arr[i+3]}
		TargetTB=${arr[i+4]}

		#写日志文件
		LOGFILE="${ShardSourceTB}_${ShardID}.log"

		sql="
			SELECT
				"${SelectCOLS}"
			FROM ${SourceDB}${v_schema}${SourceTB} ${v_nolock}
			WHERE ${Condition}"

		echo "==i=${i} Begin GetDATA FROM ${ShardID} ${SourceDB} ${SourceTB} TO ${TargetDB} ${TargetTB}==="
    {
		#sh datax_sharding_RunThreeTimes_Titan_v4.sh "${SourceDB}" "${SourceTB}" "${sql}" "${TargetDB}" "${TargetTB}" "${SQLSERVERorMYSQL}" "${ShardID}" 1>${LOGFILE}  &
		CurrentDate=$(date +"%Y-%m-%d")

		ifok=1		#初始为错误
		runnum=1	#运行次数不超过3次

		while [ ${ifok} -ne 0 ]		#如果运行报错则再运行
		do
			if [ ${runnum} -gt 3 ]	#判断如果运行次数大于3次则退出
			then
				break
			fi

			#sh datax_sharding_Titan_v4.sh "${SourceDB}" "${SourceTB}" "${sql}" "${TargetDB}" "${TargetTB}" "${SQLSERVERorMYSQL}" "${ShardID}"	#modify 3.
			srcType="${SQLSERVERorMYSQL}dal"

			if [ ${SQLSERVERorMYSQL} = 'SQLSERVER' ]
			then
				TitanCode="${SourceDB}_BIETL_SH"
			else
				TitanCode="${SourceDB}_ETLZS_SH"
			fi

			srcType=$(echo ${srcType} | tr '[A-Z]' '[a-z]')
			
			foldername=${TitanCode}${RANDOM}
			mkdir ${foldername}
			cd ${foldername}

			sh datax_run.sh \
				-srcallinone "${TitanCode}" \
				-srcdb ${SourceDB} \
				-srctblnames ${SourceTB} \
				-querys "${sql}" \
				-tardb ${TargetDB} \
				-tartblnames ${TargetTB} \
				-hournow ${ShardID} \
				-src ${srcType} -tar hdfs -loadtype 2
			
			ifok=`echo $?`
			
			if [ ${ifok} -eq 0 ]
			then
				break
			fi
			
			let runnum++
			sleep 60	#sleep 1分钟
			
		done
	}  &
		
	done;
	wait

	echo $[$j+1]" batch done"

done;