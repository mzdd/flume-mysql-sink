package com.mzdd.flume;

import org.apache.flume.instrumentation.SinkCounter;

/**
 * @author mzdd
 * @create 2023-03-31 17:48
 */
public class MysqlSinkCounter extends SinkCounter implements MysqlSinkMBean {

    private static final String TIMER_MYSQL_EVENT_SEND =
            "channel.mysql.event.send.time";

    private static final String COUNT_ROLLBACK =
            "channel.rollback.count";

    public MysqlSinkCounter(String name) {
        super(name);
    }

    public long addToMysqlEventSendTimer(long delta) {
        return addAndGet(TIMER_MYSQL_EVENT_SEND, delta);
    }

    public long incrementRollbackCount() {
        return increment(COUNT_ROLLBACK);
    }

    @Override
    public long getMysqlEventSendTimer() {
        return get(TIMER_MYSQL_EVENT_SEND);
    }

    @Override
    public long getRollbackCount() {
        return get(COUNT_ROLLBACK);
    }

}
