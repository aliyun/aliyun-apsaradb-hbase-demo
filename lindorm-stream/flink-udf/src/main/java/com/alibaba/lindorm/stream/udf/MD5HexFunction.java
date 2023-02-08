package com.alibaba.lindorm.stream.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5HexFunction extends ScalarFunction {

    public String eval(String originalString) throws NoSuchAlgorithmException{
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] md5bytes = md5.digest(originalString.getBytes(StandardCharsets.UTF_8));
        return toHexString(md5bytes);
    }

    public String toHexString(byte[] bytes){
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
