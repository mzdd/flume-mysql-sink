package com.mzdd.flume;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mzdd
 * @create 2023-03-29 10:23
 */
public abstract class AbstractMysqlSink extends AbstractSink implements Configurable, BatchSizeSupported {

    /**
     * 地址
     */
    protected String hostname;

    /**
     * 端口
     */
    protected String port;

    /**
     * 库名
     */
    protected String databaseName;

    /**
     * 表名
     */
    protected String tableName;

    /**
     * 用户名
     */
    protected String user;

    /**
     * 密码
     */
    protected String password;

    /**
     * 批量的插入条数，默认值 100
     */
    protected long batchSize = 100L;

    /**
     * 数据源
     */
    protected DruidDataSource dataSource;

    /**
     * sql语句
     */
    protected String sql;

    /**
     * 自定义mysql监控
     */
    protected MysqlSinkCounter counter;

    /**
     * 将记录加入需要保存的list中
     *
     * @param content
     * @param saveLogList
     */
    public abstract void addLogEntity(String content, List<Object> saveLogList);

    /**
     * 创建 PreparedStatement
     *
     * @return
     * @throws SQLException
     */
    public PreparedStatement getPreparedStatement(String sql) throws SQLException {
        DruidPooledConnection connection = this.dataSource.getConnection();
        Connection conn = connection.getConnection();
        conn.setAutoCommit(false);
        return conn.prepareStatement(sql);
    }


    @Override
    public Status process() throws EventDeliveryException {
        // System.out.println("==> sink process ");
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        List<Object> logList = new ArrayList<>();
        transaction.begin();
        DruidPooledConnection connection = null;
        try {
            long processedEvents = 0L;
            long batchStartTime = System.nanoTime();

            connection = this.dataSource.getConnection();
            connection.setAutoCommit(false);
            for (; processedEvents < batchSize; processedEvents++) {
                event = channel.take();
                if (event != null) {

                    counter.incrementEventDrainAttemptCount();

                    //对事件进行处理
                    content = new String(event.getBody());
                    this.addLogEntity(content, logList);
                } else {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }
            }
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            if (logList.size() > 0) {
                preparedStatement.clearBatch();
                for (Object log : logList) {
                    Class clazz = log.getClass();
                    Field[] fs = clazz.getDeclaredFields();
                    for (int i = 0; i < fs.length; i++) {
                        Field field = fs[i];
                        field.setAccessible(true);
                        preparedStatement.setObject(i + 1, field.get(log));
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }

            // publish batch and commit.
            if (processedEvents > 0) {
                long endTime = System.nanoTime();
                counter.addToMysqlEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount(processedEvents);
            }

            //手动提交
            connection.commit();
            transaction.commit();
        } catch (Exception e) {
            counter.incrementEventWriteOrChannelFail(e);
            e.printStackTrace();
            try {
                if (transaction != null) {
                    transaction.rollback();
                    counter.incrementRollbackCount();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        } finally {
            try {
                if (transaction != null) {
                    transaction.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    @Override
    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString("databaseName");
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        tableName = context.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        Long size = context.getLong("batchSize");
        if (size != null) {
            this.batchSize = size;
        }
        // 初始化监控
        if (counter == null) {
            counter = new MysqlSinkCounter(getName());
        }
    }

    @Override
    public void start() {
        counter.start();
        // 创建数据库连接
        super.start();
        createDataSource();
    }

    @Override
    public void stop() {
        counter.stop();
        super.stop();
        // 关闭数据源
        dataSource.close();
    }

    public void createDataSource() {
        String className = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName + "?autoReconnect=true&useSSL=true&characterEncoding=utf8";
        //数据源配置
        dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        //这个可以缺省的，会根据url自动识别
        dataSource.setDriverClassName(className);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        //下面都是可选的配置
        //初始连接数，默认0
        dataSource.setInitialSize(5);
        //最大连接数，默认8
        dataSource.setMaxActive(10);
        //最小闲置数
        dataSource.setMinIdle(3);
        //获取连接的最大等待时间，单位毫秒
        dataSource.setMaxWait(20000);
        //缓存PreparedStatement，默认false
        dataSource.setPoolPreparedStatements(true);
        //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码
        dataSource.setMaxOpenPreparedStatements(20);
    }

    @Override
    public long getBatchSize() {
        return this.batchSize;
    }

}
