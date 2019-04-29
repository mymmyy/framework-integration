package com.mym.practice.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.util.HashSet;
import java.util.Set;


public class SyncUtils {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(SyncUtils.class);

	/**
	 * set值成功标识
	 */
	private final static String SET_OK = "OK";

	/**
	 * @Description: 获取redis同步锁
	 * @Title: SyncUtils
	 * @param key
	 * @param expire
	 * @return
	 * boolean
	 */
    public static boolean getSyncLock(String key, int expire) {
    	return getSyncLock(key, expire, "lock");
    }
    
    /**
     * @Description: 获取redis同步锁(setnx方式)
     * @Title: SyncUtils
     * @param key
     * @param expire
     * @param value
     * @return 
     * boolean
     */
    public static boolean getSyncLock(String key, int expire, String value) {
    	JedisCluster jedis = null;
    	try {
    		jedis = getJedisCluster();
    		long ttl = jedis.ttl(key);
    		// 避免同步锁无法释放导致程序正常运行
    		if (ttl == -1) {
    			//没有设置剩余的生存时间,del掉
    			jedis.del(key);
    		}
    		//redis实现分布式锁
        	long exist = jedis.setnx(key, value);//当前时间,设置有效时间
            if(exist == 1){ //获取到同步锁后,对此KEY设置有效期
            	jedis.expire(key, expire);
            }
            return exist == 1;
		} catch (Exception e) {
			LOGGER.error("get SyncLock key:{}, value:{} error!", key, value, e);
			return false;
		} finally {
			try {
				jedis.close();
			} catch (Exception e2) {
				LOGGER.error("close jedis error", e2);
			}
		}
    }

	/**
	 * 获取锁方式：set key value [EX seconds] [NX|XX]
	 * @param key 键
	 * @param expire 超时时间(s)。注：EX|PX, key 的存在时间: EX = seconds; PX = milliseconds
	 * @param value 值
	 * @param nxOrXx 参数为nx 还是 xx。注：NX|XX, NX --有此参数时只能set不存在的key,如果给已经存在的key set 值则不生效， XX -- 此参数只能设置已经存在的key 的值，不存在的不生效
	 * @return 获取锁是否成功
	 */
    public static boolean getSyncLockNew(String key, int expire, String value, String nxOrXx){
		JedisCluster jedis = null;
		try{
			jedis = getJedisCluster();
			SetParams setParams = new SetParams();
			setParams.ex(expire);
			if(nxOrXx.equalsIgnoreCase("nx")){
				setParams.nx();
			}else if(nxOrXx.equalsIgnoreCase("xx")){
				setParams.xx();
			}
			return SET_OK.equalsIgnoreCase(jedis.set(key, value, setParams));
		}catch (Exception e){
			LOGGER.error("get SyncLock key:{}, value:{} error!", key, value, e);
			return false;
		} finally {
			try {
				jedis.close();
			} catch (Exception e2) {
				LOGGER.error("close jedis error", e2);
			}
		}
	}

	/**
	 * 获取锁方式：set key value [EX seconds] [NX]
	 * @param key 键
	 * @param expire 超时时间（s）
	 * @return 获取锁是否成功
	 */
	public static boolean getSyncLockNewNx(String key, int expire){
		return getSyncLockNew(key, expire, "lock", "nx");
	}

	/**
	 * 这里只为测试，实际上应该从pool里获得Jedis对象
	 * @return
	 */
	public static JedisCluster getJedisCluster(){
		// 添加集群的服务节点Set集合
		Set<HostAndPort> hostAndPortsSet = new HashSet<HostAndPort>();
		// 添加节点
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7001));
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7002));
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7003));
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7004));
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7005));
		hostAndPortsSet.add(new HostAndPort("192.168.31.202", 7006));

		// Jedis连接池配置
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		// 最大空闲连接数, 默认8个
		jedisPoolConfig.setMaxIdle(100);
		// 最大连接数, 默认8个
		jedisPoolConfig.setMaxTotal(500);
		//最小空闲连接数, 默认0
		jedisPoolConfig.setMinIdle(0);
		// 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
		jedisPoolConfig.setMaxWaitMillis(2000); // 设置2秒
		//对拿到的connection进行validateObject校验
		jedisPoolConfig.setTestOnBorrow(true);
		JedisCluster jedis = new JedisCluster(hostAndPortsSet, jedisPoolConfig);

		return jedis;
	}

	/**
	 * 这里只为测试，实际上应该从pool里获得Jedis对象
	 * @return
	 */
	public static Jedis getJedis(){
		return new Jedis("192.168.31.202", 7001);
	}
	
}
