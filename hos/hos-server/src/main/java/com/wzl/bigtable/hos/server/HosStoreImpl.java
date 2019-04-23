package com.wzl.bigtable.hos.server;

import com.google.common.base.Strings;
import com.wzl.bigtable.common.HosObject;
import com.wzl.bigtable.common.HosObjectSummary;
import com.wzl.bigtable.common.ObjectListResult;
import com.wzl.bigtable.common.ObjectMetaData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class HosStoreImpl implements IHosStore {
    private static Logger logger = Logger.getLogger(HosStoreImpl.class);

    private Connection connection = null;
    private IHdfsService fileStore;
    private String zkUrls;
    private CuratorFramework zkClient;

    public HosStoreImpl(Connection connection, IHdfsService fileStore, String zkUrls, CuratorFramework zkClient) {
        this.connection = connection;
        this.fileStore = fileStore;
        this.zkUrls = zkUrls;
        zkClient = CuratorFrameworkFactory.newClient(zkUrls, new ExponentialBackoffRetry(20, 5));
        this.zkClient.start();
    }

    @Override
    public void createBucketStore(String bucket) throws IOException {
        //1.创建目录表
        HBaseServiceImpl.createTable(connection, HosUtil.getDirTableName(bucket), HosUtil.getDirColumnFamily(), null);
        //创建文件表
        HBaseServiceImpl.createTable(connection, HosUtil.getObjTableName(bucket), HosUtil.getObjColumnFamily(), HosUtil.OBJ_REGIONS);
        //将其添加到seq表
        Put put = new Put(bucket.getBytes());
        put.addColumn(HosUtil.BUCKET_DIR_SEQ_CF_BYTES, HosUtil.BUCKET_DIR_SEQ_QUALIFIER, Bytes.toBytes(0L));
        HBaseServiceImpl.putRow(connection, HosUtil.BUCKET_DIR_SEQ_TABLE, put);
        //创建hdfs目录
        fileStore.mikDir(HosUtil.FILE_STORE_ROOT + "/" + bucket);
    }


    @Override
    public void deleteBucketStore(String bucket) throws IOException {
        //删除目录表和文件表
        HBaseServiceImpl.deleTable(connection, HosUtil.getDirTableName(bucket));
        HBaseServiceImpl.deleTable(connection, HosUtil.getObjTableName(bucket));
        //删除seq表中的记录
        HBaseServiceImpl.deleRow(connection, HosUtil.BUCKET_DIR_SEQ_TABLE, bucket);
        //删除hdfs上的目录
        fileStore.deleteDir(HosUtil.FILE_STORE_ROOT + "/" + bucket);
    }

    @Override
    public void createSeqTable() throws IOException {
        HBaseServiceImpl.createTable(connection, HosUtil.BUCKET_DIR_SEQ_TABLE, new String[]{HosUtil.BUCKET_DIR_SEQ_CF}, null);
    }

    @Override
    public void put(String bucket, String key, ByteBuffer content, long length, String mediaType, Map<String, String> properties) throws Exception {
        //判断是否是创建目录
        InterProcessMutex lock = null;
        if (key.endsWith("/")) {
            putDir(bucket, key);
            return;
        }
        //获取seqid
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String hash = null;
        while (hash == null) {
            if (dirExist(bucket, dir)) {
                hash = putDir(bucket, dir);
            } else {
                hash = getDirSeqId(bucket, dir);
            }
        }
        //上传文件到文件表（去目录获取seqid）

        //获取锁
        String lockey = key.replace("/", "_");
        lock = new InterProcessMutex(zkClient, "/hos/" + bucket + "/" + lockey);
        lock.acquire();
        //上传文件
        String fileKey = hash + "_" + key.substring(key.lastIndexOf("/") + 1);
        Put contentPut = new Put(fileKey.getBytes());

        if (!Strings.isNullOrEmpty(mediaType)) {
            contentPut.addColumn(HosUtil.OBJ_META_CF_BYTES, HosUtil.OBJ_MEDIATYPE_QUALIFIER, mediaType.getBytes());
        }
        contentPut.addColumn(HosUtil.OBJ_META_CF_BYTES, HosUtil.OBJ_LEN_QUALIFIER, Bytes.toBytes((long) length));
        //判断文件的大小，大于20 上传到hdfs
        if (length <= HosUtil.FILE_STORE_THRESHOLD) {
            ByteBuffer qualifierBuffer = ByteBuffer.wrap(HosUtil.OBJ_CONT_QUALIFIER);
            contentPut.addColumn(HosUtil.OBJ_CONT_CF_BYTES, qualifierBuffer, System.currentTimeMillis(), content);
            qualifierBuffer.clear();
        } else {
            String filrDir = HosUtil.FILE_STORE_ROOT + "/" + bucket + "/hash";
            String name = key.substring(key.lastIndexOf("/") + 1);
            InputStream inputStream = new ByteBufferInputStream(content);
            fileStore.saveFile(filrDir, name, inputStream, length, (short) 1);
        }
        HBaseServiceImpl.putRow(connection, HosUtil.getObjTableName(bucket), contentPut);
        //释放锁
        if (lock != null) {
            lock.release();
        }
    }

    private String getDirSeqId(String bucket, String dir) {
        Result result = HBaseServiceImpl.getRow(connection, HosUtil.getDirTableName(bucket), dir);
        if (result.isEmpty()) {
            return null;
        }
        return Bytes.toString(result.getValue(HosUtil.DIR_META_CF_BYTES, HosUtil.DIR_SEQID_QUALIFIER));
    }

    private String putDir(String bucket, String key) throws Exception {
        if (dirExist(bucket, key)) {
            return null;
        }
        //从zk获取锁
        InterProcessMutex lock = null;
        try {
            String lockey = key.replace("/", "_");
            lock = new InterProcessMutex(zkClient, "/hos" + bucket + "/" + lockey);
            lock.acquire();
            String dir1 = key.substring(1, key.lastIndexOf("/"));
            String name = dir1.substring(dir1.lastIndexOf("/"));
            //创建目录
            if (name.length() > 0) {
                String parent = dir1.substring(1, dir1.lastIndexOf("/") + 1);
                if (!dirExist(bucket, parent)) {
                    this.putDir(bucket, parent);
                }
                Put put = new Put(Bytes.toBytes(parent));
                put.addColumn(HosUtil.DIR_SUBDIR_CF_BYTES, Bytes.toBytes(name), Bytes.toBytes("1"));
                HBaseServiceImpl.putRow(connection, HosUtil.getDirTableName(bucket), put);
            }
            //再去添加到目录表
            String seqId = getDirSeqId(bucket, key);
            String hash = seqId == null ? makeDirSeqId(bucket) : seqId;
            Put dirPut = new Put(key.getBytes());
            dirPut.addColumn(HosUtil.DIR_SUBDIR_CF_BYTES, HosUtil.DIR_SEQID_QUALIFIER, Bytes.toBytes(hash));
            HBaseServiceImpl.putRow(connection, HosUtil.getDirTableName(bucket), dirPut);
            return hash;
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
        //释放锁
    }

    private String makeDirSeqId(String bucket) {
        long v = HBaseServiceImpl.incrementColumnValue(connection, HosUtil.BUCKET_DIR_SEQ_TABLE, bucket,
                HosUtil.BUCKET_DIR_SEQ_CF_BYTES, HosUtil.BUCKET_DIR_SEQ_QUALIFIER,
                1);
        return String.format("%da%d", v % 64, v);
    }

    private boolean dirExist(String bucket, String dir) throws IOException {
        return HBaseServiceImpl.existsRow(connection, HosUtil.getDirTableName(bucket), dir);
    }

    @Override
    public HosObjectSummary getSummary(String bucket, String key) throws IOException {
        //判断是否为文件夹
        if ((key.endsWith("/"))) {
            Result result = HBaseServiceImpl.getRow(connection, HosUtil.getDirTableName(bucket), key);
            if (!result.isEmpty()) {
                //读取文件夹的基础属性 转换为 HosObjectSummary
                return this.dieObjectToSummary(result, bucket, key);
            }
            return null;
        }
        //获取文件的基本属性
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String seq = getDirSeqId(bucket, dir);

        if (seq == null) {
            return null;
        }
        String objkey = seq + "_" + key.substring(key.lastIndexOf("/") + 1);
        Result result = HBaseServiceImpl.getRow(connection, HosUtil.getObjTableName(bucket), objkey);
        if (seq.isEmpty()) {
            return null;
        }
        return this.resultToObjectSummary(result, bucket, dir);
    }

    private HosObjectSummary resultToObjectSummary(Result result, String bucket, String dir) {
        HosObjectSummary summary = new HosObjectSummary();
        long timeStap = result.rawCells()[0].getTimestamp();
        String id = new String(result.getRow());
        summary.setId(id);
        String name = id.split("_", 2)[1];
        summary.setName(name);
        summary.setKey(dir + name);
        summary.setBucket(bucket);
        summary.setMediaType(Bytes.toString(result.getValue(HosUtil.OBJ_META_CF_BYTES, HosUtil.OBJ_MEDIATYPE_QUALIFIER)));
        //
        return summary;
    }

    private HosObjectSummary dieObjectToSummary(Result result, String bucket, String dir) {
        HosObjectSummary summary = new HosObjectSummary();
        summary.setId(Bytes.toString(result.getRow()));
        summary.setAttrs(new HashMap<>(0));
        summary.setBucket(bucket);
        summary.setLastModifyTime(result.rawCells()[0].getTimestamp());
        summary.setMediaType("");
        if (dir.length() > 1) {
            summary.setName(dir.substring(dir.lastIndexOf("/") + 1));
        } else {
            summary.setName("");
        }
        return summary;
    }

    @Override
    public List<HosObjectSummary> list(String bucket, String startKey, String endKey) throws IOException {
        return null;
    }

    @Override
    public ObjectListResult listDir(String bucket, String dir, String start, int maxCount) throws IOException {
        //查询目录表
        if (start == null) {
            start = "";
        }
        Get get = new Get(Bytes.toBytes(dir));
        get.addFamily(HosUtil.DIR_SUBDIR_CF_BYTES);
        if (start.length() > 0) {
            get.setFilter(new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(start))));
        }
        int maxCount1 = maxCount + 2;
        Result dirResult = HBaseServiceImpl.getRow(connection, HosUtil.getDirTableName(bucket), get);
        List<HosObjectSummary> subDirs = null;
        //查询文件
        if (!dirResult.isEmpty()) {
            subDirs = new ArrayList<>();
            for (Cell cell : dirResult.rawCells()) {
                HosObjectSummary summary = new HosObjectSummary();
                byte[] qualifierBytes = new byte[cell.getQualifierLength()];
                CellUtil.copyQualifierTo(cell, qualifierBytes, 0);
                String name = Bytes.toString(qualifierBytes);
                summary.setKey(dir + name + "/");
                summary.setName(name);
                summary.setLastModifyTime(cell.getTimestamp());
                summary.setMediaType("");
                summary.setBucket(bucket);
                summary.setLength(0);
                subDirs.add(summary);
                if (subDirs.size() >= maxCount1) {
                    break;
                }
            }
        }
        //查询文件表
        String dirSeq = this.getDirSeqId(bucket, dir);
        byte[] objStart = Bytes.toBytes(dirSeq + "_" + start);
        Scan objScan = new Scan();
        objScan.setRowPrefixFilter(Bytes.toBytes(dirSeq + "_"));
        objScan.setFilter(new PageFilter(maxCount + 1));
        objScan.setStartRow(objStart);
        objScan.setMaxResultsPerColumnFamily(maxCount1);
        objScan.addFamily(HosUtil.OBJ_META_CF_BYTES);
        logger.info("scan start: " + Bytes.toString(objStart) + " - ");
        ResultScanner objScanner = HBaseServiceImpl.getScananer(connection, HosUtil.getObjTableName(bucket), objScan);
        List<HosObjectSummary> objectSummaryList = new ArrayList<>();
        Result result = null;
        while (objectSummaryList.size() < maxCount1 && (result = objScanner.next()) != null) {
            HosObjectSummary summary = this.resultToObjectSummary(result, bucket, dir);
            objectSummaryList.add(summary);
        }
        if (objScanner != null) {
            objScanner.close();
        }
        logger.info("scan complete: " + Bytes.toString(objStart) + " - ");
        if (subDirs != null && subDirs.size() > 0) {
            objectSummaryList.addAll(subDirs);
        }
        Collections.sort(objectSummaryList);
        ObjectListResult listResult = new ObjectListResult();
        HosObjectSummary nextMarkerObj =
                objectSummaryList.size() > maxCount ? objectSummaryList.get(objectSummaryList.size() - 1)
                        : null;
        if (nextMarkerObj != null) {
            listResult.setNextMarker(nextMarkerObj.getKey());
        }
        if (objectSummaryList.size() > maxCount) {
            objectSummaryList = objectSummaryList.subList(0, maxCount);
        }
        listResult.setMaxKeyNumber(maxCount);
        if (objectSummaryList.size() > 0) {
            listResult.setMinKey(objectSummaryList.get(0).getKey());
            listResult.setMaxKey(objectSummaryList.get(objectSummaryList.size() - 1).getKey());
        }
        listResult.setObjectCount(objectSummaryList.size());
        listResult.setObjectList(objectSummaryList);
        listResult.setBucket(bucket);

        return listResult;
    }

    @Override
    public ObjectListResult listByPrefix(String bucket, String dir, String keyPrefix, String start, int maxCount) throws IOException {
        if (start == null) {
            start = "";
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new ColumnPrefixFilter(keyPrefix.getBytes()));
        if (start.length() > 0) {
            filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(start))));
        }
        int maxCount1 = maxCount + 2;
        Result dirResult = HBaseServiceImpl.getRow(connection, HosUtil.getDirTableName(bucket), dir, filterList);
        List<HosObjectSummary> subDirs = null;
        if (!dirResult.isEmpty()) {
            subDirs = new ArrayList<>();
            for (Cell cell : dirResult.rawCells()) {
                HosObjectSummary summary = new HosObjectSummary();
                byte[] qualifierBytes = new byte[cell.getQualifierLength()];
                CellUtil.copyQualifierTo(cell, qualifierBytes, 0);
                String name = Bytes.toString(qualifierBytes);
                summary.setKey(dir + name + "/");
                summary.setName(name);
                summary.setLastModifyTime(cell.getTimestamp());
                summary.setMediaType("");
                summary.setBucket(bucket);
                summary.setLength(0);
                subDirs.add(summary);
                if (subDirs.size() >= maxCount1) {
                    break;
                }
            }
        }
        String dirSeq = this.getDirSeqId(bucket, dir);
        byte[] objStart = Bytes.toBytes(dirSeq + "_" + start);
        Scan objScan = new Scan();
        objScan.setRowPrefixFilter(Bytes.toBytes(dirSeq + "_" + keyPrefix));
        objScan.setFilter(new PageFilter(maxCount + 1));
        objScan.setStartRow(objStart);
        objScan.setMaxResultsPerColumnFamily(maxCount1);
        objScan.addFamily(HosUtil.OBJ_META_CF_BYTES);
        logger.info("scan start: " + Bytes.toString(objStart) + " - ");
        ResultScanner objScanner = HBaseServiceImpl.getScananer(connection, HosUtil.getObjTableName(bucket), objScan);
        List<HosObjectSummary> objectSummaryList = new ArrayList<>();
        Result result = null;
        while (objectSummaryList.size() < maxCount1 && (result = objScanner.next()) != null) {
            HosObjectSummary summary = this.resultToObjectSummary(result, bucket, dir);
            objectSummaryList.add(summary);
        }
        if (objScanner != null) {
            objScanner.close();
        }
        logger.info("scan complete: " + Bytes.toString(objStart) + " - ");
        if (subDirs != null && subDirs.size() > 0) {
            objectSummaryList.addAll(subDirs);
        }
        Collections.sort(objectSummaryList);
        ObjectListResult listResult = new ObjectListResult();
        HosObjectSummary nextMarkerObj =
                objectSummaryList.size() > maxCount ? objectSummaryList.get(objectSummaryList.size() - 1)
                        : null;
        if (nextMarkerObj != null) {
            listResult.setNextMarker(nextMarkerObj.getKey());
        }
        if (objectSummaryList.size() > maxCount) {
            objectSummaryList = objectSummaryList.subList(0, maxCount);
        }
        listResult.setMaxKeyNumber(maxCount);
        if (objectSummaryList.size() > 0) {
            listResult.setMinKey(objectSummaryList.get(0).getKey());
            listResult.setMaxKey(objectSummaryList.get(objectSummaryList.size() - 1).getKey());
        }
        listResult.setObjectCount(objectSummaryList.size());
        listResult.setObjectList(objectSummaryList);
        listResult.setBucket(bucket);

        return listResult;
    }

    @Override
    public HosObject getObject(String bucket, String key) throws IOException {
        //判断是否为目录
        if (key.endsWith("/")) {
            //读取目录表
            Result result = HBaseServiceImpl.getRow(connection, HosUtil.getDirTableName(bucket), key);
            if (result.isEmpty()) {
                return null;
            }
            ObjectMetaData metaData = new ObjectMetaData();
            metaData.setBucket(bucket);
            metaData.setKey(key);
            metaData.setLastModifyTime(result.rawCells()[0].getTimestamp());
            metaData.setLength(0);
            HosObject object = new HosObject();
            object.setMetaData(metaData);
            return object;
        }
        //读取文件的基本信息
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String name = key.substring(key.lastIndexOf("/") + 1);
        String seq = this.getDirSeqId(bucket, dir);
        String objKey = seq + "_" + name;
        Result result = HBaseServiceImpl.getRow(connection, HosUtil.getObjTableName(bucket), objKey);
        if (result.isEmpty()) {
            return null;
        }
        HosObject object = new HosObject();
        if (result.containsNonEmptyColumn(HosUtil.OBJ_CONT_CF_BYTES, HosUtil.OBJ_CONT_QUALIFIER)) {
            ByteArrayInputStream bas = new ByteArrayInputStream(result.getValue(HosUtil.OBJ_CONT_CF_BYTES, HosUtil.OBJ_CONT_QUALIFIER));
            object.setContent(bas);
        } else {
            String fileDir = HosUtil.FILE_STORE_ROOT + "/" + bucket + "/" + seq;
            InputStream inputStream = this.fileStore.openFile(fileDir, name);
            object.setContent(inputStream);
        }
        return object;
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        if (key.endsWith("/")) {
            //check sub dir and current dir files.
            if (!isDirEmpty(bucket, key)) {
                throw new RuntimeException("dir is not empty");
            }
            InterProcessMutex lock = null;
            try {
                String lockey = key.replaceAll("/", "_");
                lock = new InterProcessMutex(this.zkClient, "/mos/" + bucket + "/" + lockey);
                lock.acquire();
                if (!isDirEmpty(bucket, key)) {
                    throw new RuntimeException("dir is not empty");
                }
                String dir1 = key.substring(0, key.lastIndexOf("/"));
                String name = dir1.substring(dir1.lastIndexOf("/") + 1);
                if (name.length() > 0) {
                    String parent = key.substring(0, key.lastIndexOf(name));
                    HBaseServiceImpl.deleteQualifier(connection, HosUtil.getDirTableName(bucket), parent,
                                    HosUtil.DIR_SUBDIR_CF, name);
                }
                HBaseServiceImpl.deleRow(connection, HosUtil.getDirTableName(bucket), key);
                return;
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }
        //先从文件表中获取文件的length
        //通过length判断 hdfs hbase
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String name = key.substring(key.lastIndexOf("/") + 1);
        String seqId = this.getDirSeqId(bucket, dir);
        String objKey = seqId + "_" + name;
        Get get = new Get(objKey.getBytes());
        Result result = HBaseServiceImpl.getRow(connection,HosUtil.getObjTableName(bucket),get);
        if (result.isEmpty()) {
            return;
        }
        long len = Bytes.toLong(result.getValue(HosUtil.OBJ_META_CF_BYTES, HosUtil.OBJ_LEN_QUALIFIER));
        if (len > HosUtil.FILE_STORE_THRESHOLD) {
            String fileDir = HosUtil.FILE_STORE_ROOT + "/" + bucket + "/" + seqId;
            this.fileStore.deleteFile(fileDir, name);
        }
        HBaseServiceImpl.deleRow(connection, HosUtil.getObjTableName(bucket), objKey);
    }

    private boolean isDirEmpty(String bucket, String dir) throws IOException {
        return listDir(bucket, dir, null, 2).getObjectList().size() == 0;
    }


}
