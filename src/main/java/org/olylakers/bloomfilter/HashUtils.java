package org.olylakers.bloomfilter;

import org.apache.commons.codec.digest.DigestUtils;

import redis.clients.util.MurmurHash;


/**
 * Offer the offset of redis setbit using diff hash algorithm:
 * 1.MurmurHash
 * 2.SHA1 
 * @author olylakers
 *
 * 2013-12-4 上午10:44:54 
 */
public class HashUtils {
    
    /**
     * get the setbit offset by MD5
     * @param bizId
     * @return
     */
    public static int[] sha1Offset(int bizId, int hashFunctionCount, int maxBitCount) {
        int[] offsets = new int[hashFunctionCount];
        byte[] sha1 = DigestUtils.sha1(String.valueOf(bizId));
        int[] hashes = new int[4];

        hashes[0] = sha1[0] & 0xFF |
                (sha1[1] & 0xFF) << 8 |
                (sha1[2] & 0xFF) << 16 |
                (sha1[3] & 0xFF) << 24; 
        hashes[1] = sha1[4] & 0xFF |
                (sha1[5] & 0xFF) << 8 |
                (sha1[6] & 0xFF) << 16 |
                (sha1[7] & 0xFF) << 24; 
        hashes[2] = sha1[8] & 0xFF |
                (sha1[9] & 0xFF) << 8 |
                (sha1[10] & 0xFF) << 16 |
                (sha1[11] & 0xFF) << 24; 
        hashes[3] = sha1[12] & 0xFF |
                (sha1[13] & 0xFF) << 8 |
                (sha1[14] & 0xFF) << 16 |
                (sha1[15] & 0xFF) << 24;
        
        for(int i=0; i< offsets.length; i++){
            offsets[i]= (hashes[i % 2] + i * hashes[2 + (((i + (i % 2)) % 4) / 2)]) % maxBitCount;
        }        
        
        return offsets;
    }
    
    /**
     * get the setbit offset by MurmurHash
     * @param bizId
     * @return
     */
    public static int[] murmurHashOffset(long bizId, int hashFunctionCount, int maxBitCount) {
        int[] offsets = new int[hashFunctionCount];
        byte[] b = String.valueOf(bizId).getBytes();
        int hash1 = MurmurHash.hash(b, 0);
        int hash2 = MurmurHash.hash(b, hash1);
        for (int i = 0; i < hashFunctionCount; ++i){
            offsets[i] = (int) (Math.abs((hash1 + i * hash2) % maxBitCount) );
        }
        return offsets;
    }
    
    public static void main(String[] args){
        int[] offsets = murmurHashOffset(528804111363644L, 6, 1000);
        System.out.println(offsets);
    }
}
