package com.mzdd.flume;

/**
 * 定义监控返回数据
 * @author mzdd
 * @create 2023-03-31 17:50
 */
public interface MysqlSinkMBean {

    long getMysqlEventSendTimer();

    long getRollbackCount();

    long getConnectionCreatedCount();

    long getConnectionClosedCount();

    long getConnectionFailedCount();

    long getBatchEmptyCount();

    long getBatchUnderflowCount();

    long getBatchCompleteCount();

    long getEventDrainAttemptCount();

    long getEventDrainSuccessCount();

    long getStartTime();

    long getStopTime();

    String getType();

}
