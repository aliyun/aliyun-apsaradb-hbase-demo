package com.aliyun.phoenix.utils;
/**
 * This file created by mengqingyi on 2018/7/10.
 */

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author mengqingyi
 * @classDescription hbase保存校验 剔除无效字符串
 * @create 2018-07-10 14:58
 **/
public class HbaseStringFilter {

    /**
     * 正则表达式 新增时 去除特殊字符串
     */
    public static String stringFilter(String str) throws PatternSyntaxException {
        if (str == null) {
            str = "";
        }
        String regEx = "[`~!@#$%^&()+=|{}':;',//[//].<>/?~！@#￥%……&（）——+|{}【】‘；：”“’。，、？\\\\]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }

}
