package org.olylakers.cBloomfilter;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilter implements Cloneable, Serializable {
    private static final long serialVersionUID = -751339780541384687L;
    
    protected BitSet bloom;
	protected int k;
	protected int m;
	protected final static int seed32 = 89478583;

	/**
	 * 计算bloomFilter的max bit size
	 * @param maxNum bloomfilter期望放入的元素最大个数
	 * @param errorRate 容错率
	 * @return
	 */
	public static int optimalM(double maxNum, double errorRate) {
		return (int) Math.ceil(-1 * maxNum * Math.log(errorRate) / Math.pow(Math.log(2), 2));
	}

    /**
     * 计算bloomFilter的k
     * @param maxNum bloomfilter期望放入的元素最大个数
     * @param maxBitSize bloomfilter的bits
     * @return
     */
	public static int optimalK(double maxNum, int maxBitSize) {
		return (int) Math.ceil(Math.log(2) * maxBitSize / maxNum);
	}

	public BloomFilter(double maxNum, double errorRate) {
		this(optimalM(maxNum, errorRate), optimalK(maxNum, optimalM(maxNum, errorRate)));
	}

	public BloomFilter(int m, int k) {
		this(new BitSet(m), m, k);
	}

	
	protected BloomFilter(BitSet bloom, int m, int k) {
		this.m = m;
		this.bloom = bloom;
		this.k = k;
	}
	
	protected BloomFilter() {
		
	}

	public boolean add(long value) {
	    int[] positions =  hashMurmur(long2bytes(value));
        for (int position : positions) {
            setBit(position);
        }
        return true;
	}


	public void clear() {
		bloom.clear();
	}

	public boolean contains(long value) {
	    int[] positions =  hashMurmur(long2bytes(value));
		for (int position : positions)
			if (!getBit(position)){
	             return false;
			}
		return true;
	}

	protected boolean getBit(int index) {
		return bloom.get(index);
	}


	protected void setBit(int index) {
		setBit(index, true);
	}

	protected void setBit(int index, boolean to) {
		bloom.set(index, to);
	}

	public BitSet getBitSet() {
		return bloom;
	}
	
	/**
	 * 从hadoop代码里扣出来的hash 函数
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
		if (random > (2147483647 - 2147483647 % m)
				|| random == Integer.MIN_VALUE)
			return -1;
		else
			return random % m;
	}


	public synchronized boolean isEmpty() {
		return bloom.isEmpty();
	}

	public double getFalsePositiveProbability(int n) {
		return Math.pow((1 - Math.exp(-k * (double) n / (double) m)), k);
	}

	public double getBitsPerElement(int n) {
		return m / (double) n;
	}

	public double getBitZeroProbability(int n) {
		return Math.pow(1 - (double) 1 / m, k * n);
	}

	public int size() {
		return m;
	}

	public int getM() {
		return m;
	}


	public int getK() {
		return k;
	}

    public static byte[] long2bytes(long num) {
        byte[] b = new byte[8];
        for (int i=0;i<8;i++) {
            b[i] = (byte)(num>>>(56-(i*8)));
        }
        return b;
    }	
	
	@Override
	public synchronized String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Bloom Filter, Parameters ");
		sb.append("m = " + getM() + ", ");
		sb.append("k = " + getK() + ", ");
		for (int i = 0; i < m; i++) {
			sb.append(bloom.get(i) ? 1 : 0);
			sb.append("\n");
		}
		return sb.toString();
	}
}
