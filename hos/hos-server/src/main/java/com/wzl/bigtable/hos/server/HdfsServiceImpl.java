package com.wzl.bigtable.hos.server;

import com.wzl.bigtable.hos.core.ErrorCodes;
import com.wzl.bigtable.hos.core.HosConfiguration;
import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HdfsServiceImpl implements IHdfsService {
    private static Logger logger = Logger.getLogger(HdfsServiceImpl.class);

    private FileSystem fileSystem;
    private long defaulyBlockSize = 128*1024*1024;
    private long initBlockSize = defaulyBlockSize/2;

    public HdfsServiceImpl() throws Exception {
        //读取hdfs的相关信息
        String confDir = System.getenv("HADOOP_CONF_DIR");
        if (confDir == null) {
            confDir = System.getProperty("HADOOP_CONF_DIR");
        }
        if (confDir == null) {
            HosConfiguration hosConfiguration = HosConfiguration.getConfiguration();
            confDir = hosConfiguration.getString("hadoop.conf.dir");
        }
        if (!new File(confDir).exists()) {
            throw new FileNotFoundException(confDir);
        }
        //通过配置 读取一个filesystem的实例
        Configuration conf = new Configuration();
        conf.addResource(new Path(confDir + "/core-site.xml"));
        conf.addResource(new Path(confDir + "/hdfs-site.xml"));
//    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(new URI("hdfs://master:9000"), conf);
    }

    @Override
    public void saveFile(String dir, String name, InputStream input, long length, short replication) throws IOException {
        //判断dir是否存在 不存在 则新建
        Path dirPath = new Path(dir);
        try {
            if(!fileSystem.exists(dirPath)){
                boolean succ = fileSystem.mkdirs(dirPath, FsPermission.getDefault());
                if(succ){
                    logger.info("create dir"+dirPath);
                }else {
                    throw new HosServerException(ErrorCodes.ERROR_HDFS,"create dir "+dirPath + " error");
                }
            }
        }catch(FileExistsException ex){
            ex.printStackTrace();
        }
        //保存文件
        Path path = new Path(dir+"/"+name);
        long blockSize = length<=initBlockSize?initBlockSize:defaulyBlockSize;
        FSDataOutputStream outputStream = fileSystem.create(path,true,512*1024,replication,blockSize);
        try{
            fileSystem.setPermission(path,FsPermission.getFileDefault());
            byte[] buffer = new byte[512*1024];
            int len = -1;
            while ((len = input.read(buffer)) > 0){
                outputStream.write(buffer,0,len);
            }
        }finally {
            input.close();
            outputStream.close();
        }
    }

    @Override
    public void deleteFile(String dir, String name) throws IOException {
        fileSystem.delete(new Path(dir+"/"+name),false);
    }

    @Override
    public InputStream openFile(String dir, String name) throws IOException {
        return fileSystem.open(new Path(dir+"/"+name));
    }

    @Override
    public void mikDir(String dir) throws IOException {
        fileSystem.mkdirs(new Path(dir));
    }

    @Override
    public void deleteDir(String dir) throws IOException {
        fileSystem.delete(new Path(dir),true);
    }
}
