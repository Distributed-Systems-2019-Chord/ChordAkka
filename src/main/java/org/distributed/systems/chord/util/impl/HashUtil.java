package org.distributed.systems.chord.util.impl;

import org.distributed.systems.chord.util.IHashUtil;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class HashUtil implements IHashUtil {

    /**
     * Hashing with SHA1
     *
     * @param input String to hash
     * @return String hashed
     */
    @Override
    public Long hash(String input) {
        Long sha1 = null;
        try {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            crypt.update(input.getBytes("UTF-8"));
            sha1 = byteToLong(crypt.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        // TODO: Messed this UP!
//        try {
//            throw new Exception("Not Implemented - messed up by Leonard");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return 0L;
         return sha1;
    }

    private static String byteToHex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        String result = formatter.toString();
        formatter.close();
        return result;
    }

    private static long byteToLong(final byte[] hash) {
        byte[] compressed = new byte[4];
        for (int j = 0; j < 4; j++) {
            byte temp = hash[j];
            for (int k = 1; k < 5; k++) {
                temp = (byte) (temp ^ hash[j + k]);
            }
            compressed[j] = temp;
        }

        long ret = (compressed[0] & 0xFF) << 24 | (compressed[1] & 0xFF) << 16 | (compressed[2] & 0xFF) << 8 | (compressed[3] & 0xFF);
        ret = ret & (long) 0xFFFFFFFFl;
        return ret;
    }
}


