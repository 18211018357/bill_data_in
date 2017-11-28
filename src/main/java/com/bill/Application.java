package com.bill;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.bill.mr.MysqlDBOutputormat;
import com.bill.util.OpUtils;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {

		// 启动spring框架配置
		ApplicationContext context = SpringApplication.run(Application.class, args);

		OpUtils.initConfig(context);
		
		String input = OpUtils.getArgs("inputFilePath");
		String[] args0 = input.split(",");
		
		// "hdfs://ljc:9000/buaa/student/student.txt"};
		int ec;
		try {
			ec = ToolRunner.run(new Configuration(), new MysqlDBOutputormat(), args0);
			System.exit(ec);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
