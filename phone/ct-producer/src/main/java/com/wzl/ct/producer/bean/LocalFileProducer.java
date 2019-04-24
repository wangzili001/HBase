package com.wzl.ct.producer.bean;


import com.wzl.ct.common.Util.DateUtil;
import com.wzl.ct.common.Util.NumberUtil;
import com.wzl.ct.common.bean.DataIn;
import com.wzl.ct.common.bean.DataOut;
import com.wzl.ct.common.bean.Producer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件的生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    /**
     * 生产数据
     */
    @Override
    public void produce() {
        try {
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);
            while (flag){
                //从通讯录中随机查找两个电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true){
                    call2Index = new Random().nextInt(contacts.size());
                    if(call1Index != call2Index){
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);
                //生成随机通话时间
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime = DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();
                //通话时间
                long calltime = startTime + (long)((endTime-startTime)*Math.random());
                //通话时间字符串
                String callTimeString = DateUtil.format(new Date(calltime),"yyyyMMddHHmmss");
                //生成随机通话时长
                String duration = NumberUtil.format(new Random().nextInt(3000),4);
                //生成通话记录
                Calllog calllog = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);
                System.out.println(calllog);
                //将通话记录刷写到数据文件中
                out.write(calllog);
                Thread.sleep(500);
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public void setIn(DataIn in) {
        this.in = in;
    }

    @Override
    public void setOut(DataOut dataOut) {
        this.out = out;
    }

    /**
     * 关闭生产者
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if(in != null){
            in.close();
        }
        if(out != null){
            out.close();
        }
    }
}
