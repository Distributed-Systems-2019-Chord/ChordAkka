package org.distributed.systems.chord.util.impl;

import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.util.IHashUtil;

import java.nio.charset.StandardCharsets;
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
        try {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            crypt.update(input.getBytes(StandardCharsets.UTF_8));
            return Math.floorMod(byteToLong(crypt.digest()), ChordStart.AMOUNT_OF_KEYS);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
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
        ret = ret & 0xFFFFFFFFL;
        return ret;
    }
}


