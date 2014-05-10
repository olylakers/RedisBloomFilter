package com.taobao.tpn.count.bloomfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 场景的count bloomfilter是用两个bitset，一个存放bloom，一个存放count
 * 这个实现通过一个long数组来实现count bloomfilter，把count和bloom信息存放在一起
 * count bloomfilter
 * 
 * @author hantong
 * 
 */
public final class CountingBloomFilter {

	private final static int seed32 = 89478583;
	private final static int HASH_LOCK_SIZE = 16;
	
	//可以用ReentrantLock或者原子变量来实现并发控制
	//此外可以先对bizId做hash，把不同的bizId等分到不同的bloomfilter，那样并发冲突的几率会更低
	//用原子变量来做并发控制，线上测试了下，先判断contains，如果contains不存在，则add。整个操作只需要0.006ms一次，
	//用ReentrantLock也差不多是这个数量级的消耗；
	//现在最大的问题不在于时间消耗，而在于内存使用，我们现在需要进行bizId去重的消息有近5kw，即需要add 5kw；
	//虽然用户标记为已读就会remove掉，但就算最坏的情况下，只有20%的bizId没有被标记为已读，那每天也有1kw，
	//按照一般的经验，去hash函数个数为10，那么bloomfilter的m是maxNum的是20倍时，误差率在十万分之一的级别
	//但这个内存占用也是非常大的：1kw*20*4/(8*1024*1024)=96M,一天就需要近百M内存，这个基本不可能放java堆内了
	//所以如果要用count bloomfilter的话，也只能考虑redis之类的集中bloomfilter，然后通过对user_id或者biz_id取模，把请求分散到不同的redis来降低并发压力
	private ReentrantLock[] hashLocks = new ReentrantLock[HASH_LOCK_SIZE];
	private AtomicBoolean[] hashBooleans = new AtomicBoolean[HASH_LOCK_SIZE];

	/**
	 * 存放count信息的数组
	 */
	private long[] buckets;

	/**
	 * 存放count信息的数组长度
	 */
	private int maxBitSize;

	/**
	 * hash函数个数
	 */
	private int k;

	/**
	 * 一般来说只要用4个bit来存放计数信息，就可以在获取得极低的误差
	 */
	private final static long BUCKET_MAX_VALUE = 15;

	public CountingBloomFilter() {
	}

	/**
	 * 指定bloomfilter的bit的最大值和哈希函数个数
	 * @param maxBitSize
	 * @param hashFunctionNum
	 */
	public CountingBloomFilter(int maxBitSize, int hashFunctionNum) {
		buckets = new long[buckets2words(maxBitSize)];
		this.maxBitSize = maxBitSize;
		this.k = hashFunctionNum;
//		initLocks();
		initAtmoicBoolean();	
	}
	
	/**
	 * 指定bloomfilter最大可add进去的元素个数的和容错率
	 * @param maxExceptNum
	 * @param errorRate
	 */
	public CountingBloomFilter(double maxExceptNum, float errorRate) {
		this.maxBitSize = optimalM(maxExceptNum, errorRate);
		this.k = optimalK(maxExceptNum, maxBitSize);
		buckets = new long[buckets2words(maxBitSize)];
//		initLocks();
		initAtmoicBoolean();	
	}
	
	@SuppressWarnings("unused")
	private void initLocks(){
		for (int index =0; index < HASH_LOCK_SIZE; index++) {
			hashLocks[index] = new ReentrantLock();
		}
	}
	
	private void initAtmoicBoolean(){
		for (int index =0; index < HASH_LOCK_SIZE; index++) {
			hashBooleans[index] = new AtomicBoolean();
		}
	}


	/**
	 * 普通的bloomfilter是以bit来来保存信息，count bloomfilter用4个bit来保存count信息，
	 * 所以内存占用是普通bloomfilter的4倍，4*maxBitsize，因为我们用long数组来保存计数的bucket，
	 * long是64位的，所以这里需要除以16
	 * @param maxBitSize
	 * @return
	 */
	private static int buckets2words(int maxBitSize) {
		return ((maxBitSize - 1) >>> 4) + 1;
	}

	/**
	 * 计算bloomFilter的max bit size
	 * 
	 * @param maxNum bloomfilter期望放入的元素最大个数
	 * @param errorRate 容错率
	 * @return
	 */
	public static int optimalM(double maxNum, double errorRate) {
		return (int) Math.ceil(-1 * maxNum * Math.log(errorRate)
				/ Math.pow(Math.log(2), 2));
	}

	/**
	 * 计算bloomFilter的k
	 * 
	 * @param maxNum  bloomfilter期望放入的元素最大个数
	 * @param maxBitSize bloomfilter的bits
	 * @return
	 */
	public static int optimalK(double maxNum, int maxBitSize) {
		return (int) Math.ceil(Math.log(2) * maxBitSize / maxNum);
	}

	public void add(long bizId) {
		int[] hashes = hashMurmur(long2bytes(bizId));
		
		boolean reuse = false;
		for (int i = 0; i < k; i++) {
			// 找到对应的桶
			int wordNum = hashes[i] >> 4; // 除以16，一个long有64个bit，用4个bit来保持count信息，long数组的每一个元素能记录16个hash位置的count信息
			int bucketShift = (hashes[i] & 0x0f) << 2; // 模16，然后乘以4

			long bucketMask = 15L << bucketShift;
			boolean isExecute = false;
			//如果是重用的，则不需要进行compare and set
			while (!isExecute && (reuse || !reuse && hashBooleans[hashes[i]&0x0f].compareAndSet(false, true))) {
				long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;

				//只有在count值未超过BUCKET_MAX_VALUE时，才可以加1
				if (bucketValue < BUCKET_MAX_VALUE) {
					//count加1
					buckets[wordNum] = (buckets[wordNum] & ~bucketMask)
							| ((bucketValue + 1) << bucketShift);
				}
				
				//如果接下来的hash和之前的hash是一样的，那就继续使用
				if(i>=k-1 || hashes[i+1]!=hashes[i]){
					reuse = false;
					hashBooleans[hashes[i]&0x0f].compareAndSet(true, false);
				}else{
					reuse = true;
				}
				
				isExecute = true;
			}
		}
	}

	public void remove(long bizId) {
		if (contains(bizId)) {
			int[] hashes = hashMurmur(long2bytes(bizId));
			boolean reuse = false;

			for (int i = 0; i < k; i++) {
				// 找到对应的桶
				int wordNum = hashes[i] >> 4; // 除以16
				int bucketShift = (hashes[i] & 0x0f) << 2; // 模16，然后乘以4

				long bucketMask = 15L << bucketShift;
				boolean isExecute = false;
				//如果是重用的，则不需要进行compare and set
				while (!isExecute && (reuse || !reuse && hashBooleans[hashes[i]&0x0f].compareAndSet(false, true))) {
					long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;

					//只有在count值在[1,BUCKET_MAX_VALUE)之间时，才可以减1
					if (bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE) {
						// count减1
						buckets[wordNum] = (buckets[wordNum] & ~bucketMask)
								| ((bucketValue - 1) << bucketShift);
						hashBooleans[HASH_LOCK_SIZE].compareAndSet(true, false);
					}
					
					//如果接下来的hash和之前的hash是一样的，那就继续使用
					if(i>=k-1 || hashes[i+1]!=hashes[i]){
						reuse = false;
						hashBooleans[hashes[i]&0x0f].compareAndSet(true, false);
					}else{
						reuse = true;
					}
					
					isExecute = true;
				}
			}
		}
	}

	public boolean contains(long bizId) {

		int[] hashes = hashMurmur(long2bytes(bizId));

		for (int i = 0; i < k; i++) {
			// 找到对应的桶
			int wordNum = hashes[i] >> 4; // 除以16
			int bucketShift = (hashes[i] & 0x0f) << 2; // 模16，然后乘以4

			long bucketMask = 15L << bucketShift;

			if ((buckets[wordNum] & bucketMask) == 0) {
				return false;
			}
		}

		return true;
	}

	/**
	 * 估算一个key被添加了多少次
	 * 
	 * @param bizId
	 * @return
	 */
	public int approximateCount(long bizId) {
		int res = Integer.MAX_VALUE;
		int[] hashes = hashMurmur(long2bytes(bizId));
		for (int i = 0; i < k; i++) {
			// 找到对应的桶
			int wordNum = hashes[i] >> 4; // 除以16
			int bucketShift = (hashes[i] & 0x0f) << 2; // 模16，乘以4

			long bucketMask = 15L << bucketShift;
			long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
			if (bucketValue < res)
				res = (int) bucketValue;
		}
		if (res != Integer.MAX_VALUE) {
			return res;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		for (int i = 0; i < maxBitSize; i++) {
			if (i > 0) {
				res.append(" ");
			}

			int wordNum = i >> 4; // 除以16
			int bucketShift = (i & 0x0f) << 2; // 模16，然后乘以4

			long bucketMask = 15L << bucketShift;
			long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;

			res.append(bucketValue);
		}

		return res.toString();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.maxBitSize);
		int sizeInWords = buckets2words(maxBitSize);
		for (int i = 0; i < sizeInWords; i++) {
			out.writeLong(buckets[i]);
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.maxBitSize = in.readInt();
		int sizeInWords = buckets2words(this.maxBitSize);
		buckets = new long[sizeInWords];
		for (int i = 0; i < sizeInWords; i++) {
			buckets[i] = in.readLong();
		}
	}

	/**
	 * 从hadoop代码里扣出来的hash 函数
	 * 
	 * @param value
	 * @return
	 */
	protected int[] hashMurmur(byte[] value) {
		int[] positions = new int[k];

		int hashes = 0;
		int lastHash = 0;
		byte[] data = (byte[]) value.clone();
		while (hashes < k) {
			for (int i = 0; i < value.length; i++) {
				if (data[i] == 127) {
					data[i] = 0;
					continue;
				} else {
					data[i]++;
					break;
				}
			}

			// 'm' and 'r' are mixing constants generated offline.
			// They're not really 'magic', they just happen to work well.
			int m = 0x5bd1e995;
			int r = 24;

			// Initialize the hash to a 'random' value
			int len = data.length;
			int h = seed32 ^ len;

			int i = 0;
			while (len >= 4) {
				int k = data[i + 0] & 0xFF;
				k |= (data[i + 1] & 0xFF) << 8;
				k |= (data[i + 2] & 0xFF) << 16;
				k |= (data[i + 3] & 0xFF) << 24;

				k *= m;
				k ^= k >>> r;
				k *= m;

				h *= m;
				h ^= k;

				i += 4;
				len -= 4;
			}

			switch (len) {
			case 3:
				h ^= (data[i + 2] & 0xFF) << 16;
			case 2:
				h ^= (data[i + 1] & 0xFF) << 8;
			case 1:
				h ^= (data[i + 0] & 0xFF);
				h *= m;
			}

			h ^= h >>> 13;
			h *= m;
			h ^= h >>> 15;

			lastHash = rejectionSample(h);
			if (lastHash != -1) {
				positions[hashes++] = lastHash;
			}
		}
		return positions;
	}

	protected int rejectionSample(int random) {
		random = Math.abs(random);
		if (random > (2147483647 - 2147483647 % maxBitSize)
				|| random == Integer.MIN_VALUE)
			return -1;
		else
			return random % maxBitSize;
	}

	public static byte[] long2bytes(long num) {
		byte[] b = new byte[8];
		for (int i = 0; i < 8; i++) {
			b[i] = (byte) (num >>> (56 - (i * 8)));
		}
		return b;
	}
	
	public static void main(String[] args) throws InterruptedException {
//		CountingBloomFilter countingBloomFilter = new CountingBloomFilter(10000, 0.0001f);
//		countingBloomFilter.add(213131313L);
//		System.out.println(countingBloomFilter.contains(213131313L));
//		Thread.sleep(100000000l);
		long maxNum = 10000000;
		int mod = 1;
		int maxBitSize = optimalM(maxNum/mod, 0.00001f);
		int k = optimalK(maxNum/mod, maxBitSize);
		System.out.println(maxNum/mod);
		System.out.println(maxBitSize);
		System.out.println(k);
		System.out.println(maxBitSize*4L/8/1024/1024);
	}
}
