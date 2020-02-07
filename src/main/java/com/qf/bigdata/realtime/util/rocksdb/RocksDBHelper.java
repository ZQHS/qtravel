package com.qf.bigdata.realtime.util.rocksdb;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * RocksDB工具类
 */
public class RocksDBHelper implements Serializable {

    private static RocksDB dbInstance = null;
    private static Options options = null;
    //private static String dbPath = "D:/qfBigWorkSpace/qtravel/src/main/resources/rocksdb/qf";
    private static String dbPath = "/opt/framework/rocksdb/rocksdb_data/qf_rocksdb";

    public static Boolean CREATE_IF_MISSING = true;
    public static Integer BUFFER_SIZE_WRITE = 8;
    public static Integer BUFFER_SIZE_WRITE_MAXNUMBER = 5;
    public static Integer BACKGROUND_COMPACTIONS_MAX = 10;


    static {
        RocksDB.loadLibrary();
    }

    /**
     * RocksDB连接参数
     * @return
     * @throws Exception
     */
    public static Options createOptions() throws Exception{
        if(null == options){
            options = new Options();

            Filter bloomFilter = new BloomFilter(10);
            ReadOptions readOptions = new ReadOptions().setFillCache(false);
            Statistics stats = new Statistics();
            RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10);

            //通用参数
            options.setCreateIfMissing(CREATE_IF_MISSING) //是否不存在自建
                    .setStatistics(stats)
                    .setMemTableConfig(new SkipListMemTableConfig())//memtable实现方式
                    .setWriteBufferSize(BUFFER_SIZE_WRITE * SizeUnit.KB) //写缓存大小
                    .setMaxWriteBufferNumber(BUFFER_SIZE_WRITE_MAXNUMBER) //最大写缓存数量
                    .setMaxBackgroundCompactions(BACKGROUND_COMPACTIONS_MAX)//最大背后压缩数量
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)//压缩方式
                    .setCompactionStyle(CompactionStyle.UNIVERSAL)//压缩种类
                    .setTableFormatConfig(new BlockBasedTableConfig())//sst文件组成单位：基于Block块
                    .setRateLimiter(rateLimiter);
            /**
             * PlainTableConfig
             */
            //options.setTableFormatConfig(new PlainTableConfig());
            // Plain-Table requires mmap read
            // options.setAllowMmapReads(true);

            //memtable参数配置
//            options.setMemTableConfig(
//                    new HashSkipListMemTableConfig()
//                            .setHeight(4)
//                            .setBranchingFactor(4)
//                            .setBucketCount(2000000));

            //块配置
            final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            table_options.setBlockSize(64 * SizeUnit.KB)
                    .setBlockCache(new LRUCache(8 * SizeUnit.MB))
                    .setBlockSizeDeviation(5)
                    .setBlockRestartInterval(10)
                    .setCacheIndexAndFilterBlocks(true);
            options.setTableFormatConfig(table_options);
            assert (options.tableFactoryName().equals("BlockBasedTable"));
        }
        return options;
    }


    /**
     * RocksDB连接
     * @return
     * @throws Exception
     */
    public static RocksDB connect(String dbPath) throws Exception{
        if(null == options){
            options = createOptions();
        }
        if(null == dbInstance){
            dbInstance = RocksDB.open(options, dbPath);
        }

        RocksDB db = RocksDB.open(options, dbPath);
        return db;
    }


    /**
     * 操作测试
     * @throws Exception
     */
    public static void operate() throws Exception {
        RocksDB db = connect(dbPath);

        db.put("hello".getBytes(), "world".getBytes());
        byte[] value = db.get("hello".getBytes());
        System.out.format("Get('hello') = %s\n", new String(value));

        final String str = db.getProperty("rocksdb.stats");
        assert (str != null && !str.equals(""));
    }


    /**
     * 批量操作
     * @throws Exception
     */
    public static void batch() throws Exception {
        RocksDB db = connect(dbPath);
        WriteOptions writeOpt = new WriteOptions();
        for (int i = 10; i <= 19; ++i) {
            final WriteBatch batch = new WriteBatch();
            for (int j = 10; j <= 19; ++j) {
                batch.put(String.format("%dx%d", i, j).getBytes(), String.format("%d", i * j).getBytes());
            }
            db.write(writeOpt, batch);
        }
    }


    /**
     * 读操作测试
     * @throws Exception
     */
    public static void readOperate() throws Exception {
        ReadOptions readOptions = new ReadOptions().setFillCache(false);
        RocksDB db = connect(dbPath);
        db.put("hello".getBytes(), "world".getBytes());
        byte[] value = db.get("hello".getBytes());
        System.out.format("Get('hello') = %s\n", new String(value));

        value = db.get(readOptions, "world".getBytes());
        assert (value == null);

        final byte[] insufficientArray = new byte[10];
        final byte[] enoughArray = new byte[50];
        int len;
        final byte[] testKey = "asdf".getBytes();
        final byte[] testValue = "asdfghjkl;'?><MNBVCXZQWERTYUIOP{+_)(*&^%$#@".getBytes();
        db.put(testKey, testValue);
        len = db.get(testKey, insufficientArray);
        assert (len > insufficientArray.length);
        len = db.get("asdfjkl;".getBytes(), enoughArray);
        assert (len == RocksDB.NOT_FOUND);
        len = db.get(testKey, enoughArray);
        assert (len == testValue.length);

        len = db.get(readOptions, testKey, insufficientArray);
        assert (len > insufficientArray.length);
        len = db.get(readOptions, "asdfjkl;".getBytes(), enoughArray);
        assert (len == RocksDB.NOT_FOUND);
        len = db.get(readOptions, testKey, enoughArray);
        assert (len == testValue.length);
    }


    /**
     * 写操作测试
     * @throws Exception
     */
    public static void writeOperate() throws Exception {
        ReadOptions readOptions = new ReadOptions().setFillCache(false);
        RocksDB db = connect(dbPath);
        db.put("hello".getBytes(), "world".getBytes());
        byte[] value = db.get("hello".getBytes());
        System.out.format("Get('hello') = %s\n", new String(value));

        value = db.get(readOptions, "world".getBytes());
        assert (value == null);

        final byte[] insufficientArray = new byte[10];
        final byte[] enoughArray = new byte[50];
        int len;
        final byte[] testKey = "asdf".getBytes();
        final byte[] testValue = "asdfghjkl;'?><MNBVCXZQWERTYUIOP{+_)(*&^%$#@".getBytes();
        db.put(testKey, testValue);
        len = db.get(testKey, insufficientArray);
        assert (len > insufficientArray.length);
        len = db.get("asdfjkl;".getBytes(), enoughArray);
        assert (len == RocksDB.NOT_FOUND);
        len = db.get(testKey, enoughArray);
        assert (len == testValue.length);

        len = db.get(readOptions, testKey, insufficientArray);
        assert (len > insufficientArray.length);
        len = db.get(readOptions, "asdfjkl;".getBytes(), enoughArray);
        assert (len == RocksDB.NOT_FOUND);
        len = db.get(readOptions, testKey, enoughArray);
        assert (len == testValue.length);
    }


    public static void main(final String[] args) throws Exception {
        operate();
    }


}
