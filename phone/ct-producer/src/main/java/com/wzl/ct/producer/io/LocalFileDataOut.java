package com.wzl.ct.producer.io;

import com.wzl.ct.common.bean.DataOut;

import java.io.*;

/**
 * 本地文件数据输出
 */
public class LocalFileDataOut implements DataOut {
    private PrintWriter writer = null;

    public LocalFileDataOut(String path){
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object data) throws IOException {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     * @param data
     * @throws IOException
     */
    @Override
    public void write(String data) throws IOException {
        writer.println(data);
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if(writer!=null){
            writer.close();
        }
    }
}
