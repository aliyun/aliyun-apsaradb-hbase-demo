package cn.alibaba.mob.common.dto;

public enum TmobRegion {

    HZ("hz"),
    SH("sh");

    private String val;

    private TmobRegion(String val) {
        this.val = val;
    }

    public String toString() {
        return this.val;
    }

    public static TmobRegion parseValue(String str) {
        TmobRegion[] regions = values();
        int len = regions.length;

        for(int i = 0; i < len; ++i) {
            TmobRegion region = regions[i];
            if (region.val.equals(str)) {
                return region;
            }
        }

        return null;
    }

}
