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
/**
 * RocksDB工具类
 */
public class RocksDBHelper implements Serializable {

    private static RocksDB dbInstance = null;
    private static Options options = null;
    //private static String dbPath = "/opt/framework/rocksdb/rocksdb_data/qf_rocksdb";

    //公共参数
    public static Boolean CREATE_IF_MISSING = true;
    public static Integer BUFFER_SIZE_WRITE = 8;
    public static Integer BUFFER_SIZE_WRITE_MAXNUMBER = 5;
    public static Integer BACKGROUND_COMPACTIONS_MAX = 10;

    //列簇
    public static String CF_DESC_QF = "qf_cf";


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
        return dbInstance;
    }


    /**
     * 操作测试
     * @throws Exception
     */
//    public static ColumnFamilyHandle createColumnFamily(RocksDB db, String cfDesc) throws Exception {
//        //System.out.println("==============createColumnFamily===============");
//        ColumnFamilyHandle cfHandle = null;
//        if(null != db && StringUtils.isNotEmpty(cfDesc)){
//            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
//            ColumnFamilyDescriptor cfDesciptor = new ColumnFamilyDescriptor(cfDesc.getBytes(), cfOptions);
//            cfHandle = db.createColumnFamily(cfDesciptor);
//        }
//        return cfHandle;
//    }


    /**
     * 读操作测试
     * @throws Exception
     */
    public static ReadOptions readOperate(boolean fillCache) throws Exception {
        //System.out.println("==============readOperate===============");
        ReadOptions readOptions = new ReadOptions()
                .setFillCache(fillCache);
        return readOptions;
    }


    /**
     * 写操作测试
     * @throws Exception
     */
    public static WriteOptions writeOperate(boolean sync, boolean disableWAL, boolean ignoreMissCF) throws Exception {
        //System.out.println("==============writeOperate===============");
        WriteOptions writeOpts = new WriteOptions()
                .setSync(sync)
                .setDisableWAL(disableWAL);
        if(ignoreMissCF){
            writeOpts = writeOpts.setIgnoreMissingColumnFamilies(ignoreMissCF);
        }
        return writeOpts;
    }



    /**
     * 关闭连接
     * @throws Exception
     */
    public static void close(RocksDB dbInstance) throws Exception {
        //System.out.println("==============close===============");
        if(null != dbInstance){
            dbInstance.close();
        }
    }



    /**
     * 操作测试
     * @throws Exception
     */
    public static void operate(String dbPath) throws Exception {
        System.out.println("==============operate===============");
        RocksDB db = connect(dbPath);

        //写参数
        boolean sync = true;
        boolean disableWAL = false;
        boolean ignoreMissCF = false;
        WriteOptions writeOptions = writeOperate(sync, disableWAL, ignoreMissCF);


        //读参数
        boolean fillCache = true;
        ReadOptions readOptions = readOperate(fillCache);

        //1 写数据
        String singleKey = "bigdata";
        String singleValue = "hadoop,spark,flink";
        db.put(singleKey.getBytes(), singleValue.getBytes());

        String qfSingleKey = "qf";
        String qfSingleValue = "h5,java,bigdata";
        db.put(writeOptions, qfSingleKey.getBytes(), qfSingleValue.getBytes());


        //数据迭代器
        RocksIterator iterator = db.newIterator();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            iterator.status();
            String key = new String(iterator.key());
            String value = new String(iterator.value());
            System.out.println("key=" + key + ", value=" + value);
        }

        //2 读数据
        //final String stats = db.getProperty("rocksdb.stats");
        //System.out.format("rocksdb.stats = %s", stats);

        byte[] rocksDBSingleValue = db.get(singleKey.getBytes());
        System.out.format("rocksdb.singleKey = %s", new String(rocksDBSingleValue));

        byte[] rocksDBQFCFSingleValue = db.get(readOptions, qfSingleKey.getBytes());
        System.out.format("rocksdb.singleKey = %s", new String(rocksDBQFCFSingleValue));

        //3 批量操作
        WriteBatch batch = new WriteBatch();
        for (int i = 1; i <= 3; ++i) {
            String key = String.format("%d",i);
            batch.put(key.getBytes(), key.getBytes());
            db.write(writeOptions, batch);
        }

        for (int i = 1; i <= 3; ++i) {
            String key = String.format("%d",i);
            String value = new String(db.get(key.getBytes()));
            System.out.format("rocksdb.batch  [key= %s,value=%s] \n", key, value);
        }

        //关闭连接
        close(dbInstance);
    }





    public static void main(final String[] args) throws Exception {
        //String method = args[0];
        //String dbPath = args[1];

        String dbPath = "D:\\data\\rocksdb";

        //普通操作
        operate(dbPath);


    }


}
