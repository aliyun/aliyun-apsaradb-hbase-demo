package cn.alibaba.mob.common;

import java.util.Date;
import java.util.UUID;

public class TmobUtil {
	
	//mob文件最小值10k
	public final static int OBJ_HBASE_SIZE_MIN = 1024 * 10;
	
	//mob文件最大值500M
	public final static int OBJ_HBASE_SIZE_MAX = OBJ_HBASE_SIZE_MIN * 1000 * 50;
	
	public final static String OBJ_NAME = "_name";
	public final static String OBJ_SIZE = "_size";
	public final static String OBJ_TTL = "_ttl";
	public final static String OBJ_GMTCREATE = "_gmtcreate";
	public final static String OBJ_NAMESPACE = "_namespace";
	public final static String OBJ_CONTENT = "_content";
	
	public final static String COLUMN_FAMILY_DEFAULT = "cf";
	
	public final static String BUCKET_INFO_COLUMN = "binfo";
	
	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }
    
	
}
