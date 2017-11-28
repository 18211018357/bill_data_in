package com.bill.conf.init;

import java.util.Arrays;
import java.util.Properties;
import javax.annotation.Resource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

@Configuration
@ConditionalOnClass(InitConfig.class)
@EnableConfigurationProperties(InitProperties.class)
public class InitConfig {

    @Resource
    private InitProperties initProperties;

    @Bean
    public Properties loadProps(){

    	Properties props = new Properties();
    	
        for (String info:initProperties.getArgs()){
        	 String[] parts= StringUtils.split(info, ":");
             Assert.state(parts.length==2, "args shoule be defined as 'key:value', not '" + Arrays.toString(parts) + "'");
             props.put(parts[0], parts[1]);
        }
        System.out.println(props);
        return props;
    }
}
