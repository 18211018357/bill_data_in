# bill_data_in redis mysql

hdfs的文件导入mysql和redis集群


仅仅配置就可以



#redis集群配置
spring:
  redis:
    cluster:
      nodes:
        - 192.168.186.161:7000
        - 192.168.186.161:7001
        - 192.168.186.161:7002
        - 192.168.186.161:7003
        - 192.168.186.161:7004
        - 192.168.186.161:7005
        
        
        
init:
  info:
    args:
       - jdbcDriver:com.mysql.jdbc.Driver   
       - jdbcUrl:jdbc:mysql://hadoop:3306/bill?useUnicode=true&characterEncoding=utf-8
       - jdbcUsername:root
       - jdbcPasswd:root
       - tblName:bill
       - tblfields:b_key,b_value
       - redis_set:cib_bill_year_count
       - inputFilePath:hdfs://hadoop:9000/data.txt
       - jobName:cib_bill_year_account
       - split_reg:=
       
       #驱动
       #jdbc url
       #username
       #password
       #table
       #field
       #hset name
       #file path
       #job name
       #file separator
