package org.olylakers.cBloomfilter;

import java.util.BitSet;

public class CBloomFilter extends BloomFilter {
    private static final long serialVersionUID = -5067425564811641011L;
    protected BitSet counts;
    protected int bitCount;

    /**
     * 
     * @param maxNum
     * @param errorRate
     * @param bitCount
     *            用于计算count的bit位数，一般来说，4位就完全够了
     */
    public CBloomFilter(double maxNum, double errorRate, int bitCount) {
        this(optimalM(maxNum, errorRate), optimalK(maxNum,
                optimalM(maxNum, errorRate)), bitCount);
    }

    /**
     * 
     * @param m
     * @param k
     * @param bitCount
     *            用于计算count的bit位数，一般来说，4位就完全够了
     */
    public CBloomFilter(int m, int k, int bitCount) {
        this(new BitSet(m * bitCount), new BitSet(m), m, k, bitCount);
    }

    protected CBloomFilter(BitSet counts, BitSet bloom, int m, int k,
            int bitCount) {
        super(bloom, m, k);
        this.counts = counts;
        this.bitCount = bitCount;
    }

    protected CBloomFilter() {

    }

    @Override
    public boolean add(long value) {
        for (int position : hashMurmur(long2bytes(value))) {
            setBit(position);
            increment(position);
        }

        return true;
    }

    public void remove(long value) {
        if (contains(value)) {
            for (int position : hashMurmur(long2bytes(value))) {
                decrement(position);
            }
        }
    }

    /**
     * 操作内部计数器，对相应地位+1
     */
    protected void increment(int index) {
        int low = index * bitCount;
        int high = (index + 1) * bitCount;

        // 遍历改index在counter里面对应的位，直到找到第一个为0的位，然后加1
        // 比如1这个index在对应4-7位，初始的时候这4-7是0000，increment一次就变成了0001,increment两次后就变成了0010，三次就是0011
        boolean incremented = false;
        for (int i = (high - 1); i >= low; i--) {
            if (!counts.get(i)) {
                counts.set(i);
                incremented = true;
                break;
            } else {
                counts.set(i, false);
            }
        }

        // 如果超出了，就全部置为1
        if (!incremented) {
            for (int i = (high - 1); i >= low; i--) {
                counts.set(i);
            }
        }
    }

    /**
     * 操作内部计数器，对相应地位-1
     */
    protected void decrement(int index) {
        int low = index * bitCount;
        int high = (index + 1) * bitCount;

        boolean decremented = false;
        boolean nonZero = false;
        for (int i = (high - 1); i >= low; i--) {
            if (!decremented) {
                // 遍历index对应的bit位，将其反转，直到遇到第一个为1的bit，然后将其设为0
                // 比如1这个index在对应4-7位，初始的时候这4-7是0000，increment一次就变成了0001,increment两次后就变成了0010，三次就是0011
                // 那么remove一次就是0010，两次就是0001，三次就是0000
                if (counts.get(i)) {
                    counts.set(i, false);
                    decremented = true;
                } else {
                    counts.set(i, true);
                    nonZero = true;
                }
            } else {
                // 如果对应的bit位里还有为1的bit，所以这个index对应的bit位不为0
                if (counts.get(i)) {
                    nonZero = true;
                }
            }
        }

        // 如果该index对应的4个bit位全部是0了，才把bloom里面的bit设为0
        if (!nonZero)
            bloom.set(index, false);
    }

    public int getBitCount() {
        return this.bitCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Counting Bloom Filter, Parameters ");
        sb.append("m = " + getM() + ", ");
        sb.append("k = " + getK() + ", ");
        sb.append("bitCount = " + getBitCount() + "\n");
        for (int i = 0; i < m; i++) {
            sb.append(bloom.get(i) ? 1 : 0);
            sb.append(" ");
            if (counts != null) {
                for (int j = 0; j < bitCount; j++) {
                    sb.append(counts.get(bitCount * i + j) ? 1 : 0);
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public void clear() {
        super.clear();
        counts.clear();
    }

    public static void main(String[] args) {
        CBloomFilter cBloomFilter = new CBloomFilter(5, 3, 4); // m:5,k:3,bitCount:4
        long bizId1 = 12123131L;
        long bizId2 = 34123131L;
        long bizId3 = 43244234L;
        cBloomFilter.add(bizId1);
        System.out.println(cBloomFilter);
        System.out.println(cBloomFilter.contains(bizId1));

        cBloomFilter.add(bizId2);
        System.out.println(cBloomFilter);
        System.out.println(cBloomFilter.contains(bizId2));

        cBloomFilter.add(bizId3);
        System.out.println(cBloomFilter);
        System.out.println(cBloomFilter.contains(bizId3));

        cBloomFilter.remove(bizId3);
        System.out.println(cBloomFilter);
        System.out.println(cBloomFilter.contains(bizId3));
    }
}
