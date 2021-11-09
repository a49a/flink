/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @program: flink-parent
 * @author: wuren
 * @create: 2021/11/04
 */
public class JdbcRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataAsyncLookupFunction.class);
    private static final long serialVersionUID = 2L;

    private final String query;
    private final JdbcConnectionProvider connectionProvider;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private transient FieldNamedPreparedStatement statement;

    private transient Cache<RowData, List<RowData>> cache;
    private transient DataSource dataSource;

    public JdbcRowDataAsyncLookupFunction(String query) {
        this.query = query;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.cache =
                cacheMaxSize <= 0 || cacheExpireMs <= 0
                        ? null
                        : CacheBuilder.newBuilder()
                                .recordStats()
                                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                .maximumSize(cacheMaxSize)
                                .build();
        if (cache != null && context != null) {
            context.getMetricGroup()
                    .gauge("lookupCacheHitRate", (Gauge<Double>) () -> cache.stats().hitRate());
        }
        Properties properties = new Properties();
        dataSource = DruidDataSourceFactory.createDataSource(properties);
    }

    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        Connection connection;
        CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                Connection connection = dataSource.getConnection();
                                return connection;
                            } catch (SQLException e) {
                                LOG.error("", e);
                                return null;
                            }
                        })
                .thenAcceptAsync(
                        (conn) -> {
                            statement =
                                    FieldNamedPreparedStatement.prepareStatement(
                                            conn, query, keyNames);
                        });
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
