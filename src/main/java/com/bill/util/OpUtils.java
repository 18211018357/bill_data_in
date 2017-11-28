package com.bill.util;

import java.util.Properties;
import org.springframework.context.ApplicationContext;
import redis.clients.jedis.JedisCluster;

public class OpUtils {

	//jedisCluster操作
	public static JedisCluster jedisCluster; 
	private static Properties props;
	
	public static void initConfig(ApplicationContext context){
		jedisCluster = (JedisCluster)context.getBean("jedisCluster");
		props =(Properties)context.getBean("loadProps");
	}
	
	public static String getArgs(String key){
		return props.getProperty(key);
	}
}