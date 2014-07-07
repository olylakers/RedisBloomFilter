package org.olylakers.bloomfilter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * BloomFilter based on redis
 * @author olylakers
 *
 * 2013-12-4 上午9:46:37 
 */
public class BloomFilter {
    
    private String hosts;
    private int timeout;
    private int maxKey;
    private float errorRate;
    private int hashFunctionCount;
    
    private int bitSize;
    
    private ShardedJedisPool pool;
    
    private String defaultKey = "redis:bloomfilter";
    private static final String hostConfig = "127.0.0.1:6001";
    
    public BloomFilter(String hosts, int timeout, float errorRate, int maxKey){
        this.hosts = hosts;
        this.timeout = timeout;
        this.maxKey = maxKey;
        this.errorRate = errorRate;
        String[] hostInfos = hosts.split(";");
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        if(StringUtils.isNotBlank(hosts)){
            for (String hostInfo : hostInfos) {
                String[] host = hostInfo.split(":");
                if(host.length != 2){
                    throw new IllegalArgumentException("hosts should not be null or empty");
                }
                int port = 0;
                if(NumberUtils.isNumber(host[1])){
                    port = NumberUtils.toInt(host[1]);
                }
                shards.add(new JedisShardInfo(host[0],port,timeout));
            }  
        }else{
            throw new IllegalArgumentException("redis host.length != 2");
        }
        
        pool = initRedisPool(shards);
        
        bitSize = calcOptimalM(maxKey, errorRate);
        hashFunctionCount =calcOptimalK(bitSize, maxKey);
    }
    
    private ShardedJedisPool initRedisPool(List<JedisShardInfo> shards){
        Config config = new Config();
        //在借出的时候不测试有效性
        config.testOnBorrow = false;
        //在还回的时候不测试有效性
        config.testOnReturn = false;
        //最小空闲数
        config.minIdle = 8;
        //最大空闲数
        config.maxIdle = 50;
        //最大连接数
        config.maxActive = 50;
        //允许最大等待时间，2s，单位：ms
        config.maxWait = 2 * 1000;
        
        config.minEvictableIdleTimeMillis = 1000L * 60L * 60L * 5L;
        return new ShardedJedisPool(config,shards);
    }
    
    /**
     * add one object, using default key
     * @param bizId
     */
    public void add(int bizId){
        add(defaultKey, bizId);
    }
    
    /**
     * add one object using the specified key
     * @param key
     * @param bizId
     */
    public void add(String key, long bizId){
        int[] offset = HashUtils.murmurHashOffset(bizId, hashFunctionCount, bitSize);
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            for (int i : offset) {
                jedis.setbit(key, i, true);
            }
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }
    }
    
    /**
     * add one object using the specified key
     * @param key
     * @param bizId
     */
    public void addWithPipe(String key, long bizId){
        int[] offset = HashUtils.murmurHashOffset(bizId, hashFunctionCount, bitSize);
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            ShardedJedisPipeline pipeline = jedis.pipelined();
            for (int i : offset) {
                pipeline.setbit(key, i, true);
            }
            
            pipeline.sync();
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }
    }     
    
    /**
     * Check if a bizId is part of the set
     * @param key
     * @param bizId
     */
    public boolean include(String key, long bizId){
        int[] offset = HashUtils.murmurHashOffset(bizId, hashFunctionCount, bitSize);
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            for (int i : offset) {
                if(!jedis.getbit(key, i)){
                    return false;
                }
            }
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }
        
        return true;
    }
    
    /**
     * Check if a bizId is part of the set
     * @param key
     * @param bizId
     */
    public boolean includeWithPipe(String key, long bizId){
        int[] offset = HashUtils.murmurHashOffset(bizId, hashFunctionCount, bitSize);
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            ShardedJedisPipeline pipeline = jedis.pipelined();
            for (int i : offset) {
                pipeline.getbit(key, i);
            }
            
            List<Object> responses = pipeline.syncAndReturnAll();
            for (Object object : responses) {
                if(object instanceof Boolean){
                    Boolean contains = (Boolean) object;
                    if(!contains){
                        return false;
                    }
                }
            }
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }
        
        return true;
    }
    public long count(String key){
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            return jedis.bitcount(key);
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }
    }
    
    public String  getRedisData(String key){
        ShardedJedis jedis = null;
        boolean connected = true;
        try {
            jedis = pool.getResource();
            String redisData = jedis.get(key);
            return redisData;
        }finally{
            if(jedis != null){
                if(connected){
                    pool.returnResource(jedis);
                }else{
                    pool.returnBrokenResource(jedis);
                }
            }
        }        
    }
    
    /**
     * Calculate M and K
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param maxKey
     * @param errorRate
     * @return
     */
    public int calcOptimalM(int maxKey, float errorRate){
        return (int) Math.ceil(maxKey
                * (Math.log(errorRate) / Math.log(0.6185)));
    }
    
    /**
     * Calculate M and K
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param bitSize
     * @param maxKey
     * @return
     */
    public int calcOptimalK(int bitSize, int maxKey){
        return (int) Math.ceil(Math.log(2) * (bitSize / maxKey));
    }
    
    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(int maxKey) {
        this.maxKey = maxKey;
    }

    public float getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(float errorRate) {
        this.errorRate = errorRate;
    }

    public int getHashFunctionCount() {
        return hashFunctionCount;
    }

    public void setHashFunctionCount(int hashFunctionCount) {
        this.hashFunctionCount = hashFunctionCount;
    }

    public int getBitSize() {
        return bitSize;
    }

    public void setBitSize(int bitSize) {
        this.bitSize = bitSize;
    }

    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        // TODO Auto-generated method stub
        
        BloomFilter bloomFilter = new BloomFilter(hostConfig, 1000, 0.00000001f, (int)Math.pow(2, 31));
        System.out.println(bloomFilter.getBitSize()/8/1024/2014);
    }

}
