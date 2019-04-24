package com.wzl.ct.common.Util;

import java.text.DecimalFormat;
import java.util.Date;

/**
 * 数字工具类
 */
public class NumberUtil {
    /**
     * 将数字格式化为字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num,int length){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(0);
        }
        DecimalFormat df = new DecimalFormat(sb.toString());
        return df.format(num);
    }
}
