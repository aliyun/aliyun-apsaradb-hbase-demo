package cn.alibaba.mob.common.dto;

public enum TmobType {

    HBASE("hbase"),
    HDFS("hdfs");

    private String type;

    private TmobType(String type) {
        this.type = type;
    }

    public String toString() {
        return this.type;
    }

    public static TmobType parseType(String str) {
        TmobType[] values = values();
        int len = values.length;

        for(int i = 0; i < len; ++i) {
            TmobType tmobType = values[i];
            if (tmobType.type.equals(str)) {
                return tmobType;
            }
        }

        return null;
    }

}
