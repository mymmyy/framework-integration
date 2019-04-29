package com.mym.sparkproject.test;


import com.mym.sparkproject.conf.ConfigurationManager;

/**
 * 配置管理组件测试类
 * @author Administrator
 *
 */
public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String testkey1 = ConfigurationManager.getProperty("jdbc.driver");
		String testkey2 = ConfigurationManager.getProperty("jdbc.datasource.size");
		System.out.println(testkey1);  
		System.out.println(testkey2);  
	}
	
}
