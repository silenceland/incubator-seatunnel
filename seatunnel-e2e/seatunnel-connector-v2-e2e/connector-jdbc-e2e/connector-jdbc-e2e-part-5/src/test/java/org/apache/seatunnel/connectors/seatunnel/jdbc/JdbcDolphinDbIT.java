/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class JdbcDolphinDbIT extends AbstractJdbcIT {

    private static final String DOLPHINDB_IMAGE = "dolphindb/dolphindb:v2.00.10.8";
    private static final String DOLPHINDB_CONTAINER_HOST = "e2e_dolphinDb";
    private static final String DOLPHINDB_DATABASE = "\"dfs://seatunnel\"";
    private static final String DOLPHINDB_SOURCE = "e2e_source";
    private static final String DOLPHINDB_SINK = "e2e_sink";

    private static final String DOLPHINDB_USERNAME = "admin";
    private static final String DOLPHINDB_PASSWORD = "123456";
    private static final int DOLPHINDB_PORT = 8848;

    private static final String DOLPHINDB_URL =
            "jdbc:dolphindb://" + HOST + ":%s?user=%s&password=%s";

    private static final String DRIVER_CLASS = "com.dolphindb.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_dolphindb_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table %s \n"
                    + "(\n"
                    + "col1 STRING,\n"
                    + "col2 BOOL,\n"
                    + "col3 CHAR,\n"
                    + "col4 SHORT,\n"
                    + "col5 INT,\n"
                    + "col6 LONG,\n"
                    + "col7 DATE,\n"
                    + "col8 MONTH,\n"
                    + "col9 TIME,\n"
                    + "col10 MINUTE,\n"
                    + "col11 SECOND,\n"
                    + "col12 DATETIME,\n"
                    + "col13 TIMESTAMP,\n"
                    + "col14 NANOTIME,\n"
                    + "col15 NANOTIMESTAMP,\n"
                    + "col16 FLOAT,\n"
                    + "col17 DOUBLE,\n"
                    + "col17 SYMBOL,\n"
                    + "col18 UUID,\n"
                    + "col19 ANY,\n"
                    + "col20 ANY DICTIONARY,\n"
                    + "col21 DATEHOUR,\n"
                    + "col22 IPADDR,\n"
                    + "col23 INT128,\n"
                    + "col24 BLOB,\n"
                    + "col25 DECIMAL32(3)\n"
                    + "col26 DECIMAL64(3),\n"
                    + "col27 DECIMAL128(3)\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl =
                String.format(
                        DOLPHINDB_URL, DOLPHINDB_PORT, DOLPHINDB_USERNAME, DOLPHINDB_PASSWORD);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();
        String insertSql = insertTable(DOLPHINDB_DATABASE, DOLPHINDB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DOLPHINDB_IMAGE)
                .networkAliases(DOLPHINDB_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DOLPHINDB_PORT)
                .localPort(DOLPHINDB_PORT)
                .jdbcTemplate(DOLPHINDB_URL)
                .jdbcUrl(jdbcUrl)
                .userName(DOLPHINDB_USERNAME)
                .password(DOLPHINDB_PASSWORD)
                .database(DOLPHINDB_DATABASE)
                .sourceTable(DOLPHINDB_SOURCE)
                .sinkTable(DOLPHINDB_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult(String executeKey) throws SQLException, IOException {}

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/dolphindb/jdbc/2.00.11.0/jdbc-2.00.11.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10",
                    "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col17", "col18",
                    "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f1_%s", i),
                                Boolean.TRUE,
                                String.format("f1_char_%s", i),
                                Short.valueOf("1"),
                                Integer.parseInt("1"),
                                Long.parseLong("1"),
                                LocalDate.now(),
                                LocalDate.now().getMonth(),
                                LocalDateTime.now(),
                                LocalDateTime.now().getMinute(),
                                LocalDateTime.now().getSecond(),
                                LocalDateTime.now(),
                                new Timestamp(System.currentTimeMillis()),
                                LocalDateTime.now().getNano(),
                                new Timestamp(System.currentTimeMillis()),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                String.format("f1_symbol_%s", i),
                                UUID.randomUUID(),
                                "(1,2,3)",
                                "{a:1,b:2}",
                                LocalDateTime.now().getHour(),
                                "127.0.0.1",
                                Integer.parseInt("100"),
                                "test".getBytes(),
                                BigDecimal.valueOf(i, 9),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 38)
                            });
            rows.add(row);
        }
        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DOLPHINDB_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DOLPHINDB_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DOLPHINDB_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", DOLPHINDB_PORT, DOLPHINDB_PORT)));

        return container;
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql =
                "CREATE DATABASE "
                        + DOLPHINDB_DATABASE
                        + " partitioned by VALUE(1..10), HASH([SYMBOL, 40]), engine='TSDB'";
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }
}
