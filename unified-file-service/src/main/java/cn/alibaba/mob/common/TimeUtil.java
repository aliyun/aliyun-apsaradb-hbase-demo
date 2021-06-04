package cn.alibaba.mob.common;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

    public final static String format_0 = "yyyyMMddHHmmss";

    public final static String format_1 = "yyyy-MM-dd HH:mm:ss";

    public static Date parse(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(format_0);
        return format.parse(dateStr);
    }

}
