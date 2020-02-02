package com.qf.bigdata.realtime.util.uid;

import java.io.Serializable;

/**
 * 唯一id工具类
 * 参考snowflake算法
 */
public class QUIDService implements Serializable {

    private final long twepoch = 1288834974657L;
    private final long accessBits = 5L;
    private final long userBits = 5L;
    private final long durationBits = 5L;
    private final long maxAccessId = -1L ^ (-1L << accessBits);
    private final long maxUserId = -1L ^ (-1L << userBits);
    private final long maxDurationId = -1L ^ (-1L << durationBits);

    private final long sequenceBits = 12L;
    private final long accessShift = sequenceBits;
    private final long userShift = sequenceBits + accessBits;
    private final long durationShift = sequenceBits + accessBits + userBits;
    private final long timestampLeftShift = sequenceBits + accessBits + userBits + durationBits;
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private long accesses;
    private long users;
    private long durations;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public QUIDService(long accesses, long users, long durations) {
        if (accesses > maxAccessId || accesses < 0) {
            throw new IllegalArgumentException(String.format("accesses Id can't be greater than %d or less than 0", maxAccessId));
        }
        if (users > maxUserId || users < 0) {
            throw new IllegalArgumentException(String.format("users Id can't be greater than %d or less than 0", maxUserId));
        }
        if (durations > maxDurationId || durations < 0) {
            throw new IllegalArgumentException(String.format("durations Id can't be greater than %d or less than 0", maxDurationId));
        }
        this.accesses = accesses;
        this.users = users;
        this.durations = durations;
    }

    /**
     * uid生成
     * @return
     */
    public synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return ((timestamp - twepoch) << timestampLeftShift) | (accesses << accessShift) | (users << userShift) | (durations << durationShift) | sequence;
    }


    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public static void main(String[] args) {
        long accesses = 12345678;
        long users = 123456;
        long durations = 12345;

        QUIDService idWorker = new QUIDService(accesses, users, durations);
        for (int i = 0; i < 100; i++) {
            long id = idWorker.nextId();
            System.out.println(id);
        }
    }


}
