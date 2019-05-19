package cn.edu.bupt.zzy.integration.kafka;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * @description: 时间解析工具类
 * @author: zzy
 * @date: 2019/5/19
 **/
public class DateUtils {

    private DateUtils() {}

    private static DateUtils instance;

    // 单例模式
    public static DateUtils getInstance() {
        if (instance == null) {
            instance = new DateUtils();
        }
        return instance;
    }

    // 使用线程安全的FastDateFormat，而不是SimpleDateFormat
    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public long getTime(String time) throws Exception {
        //[2019-05-19 18:43:23]
        return format.parse(time.substring(1, time.length() - 1)).getTime();
    }

}
