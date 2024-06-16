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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dolphindb;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DolphinDBTypeMapper implements JdbcDialectTypeMapper {

    // ref: https://docs.dolphindb.cn/zh/help/DataTypesandStructures/DataTypes/index.html#
    // ============================data types=====================
    private static final String DOLPHINDB_UNKNOWN = "UNKNOWN";
    private static final String DOLPHINDB_VOID = "VOID";
    private static final String DOLPHINDB_BOOL = "BOOL";
    private static final String DOLPHINDB_UUID = "UUID";

    // -------------------------number----------------------------
    private static final String DOLPHINDB_SHORT = "SHORT";
    private static final String DOLPHINDB_INT = "INT";
    private static final String DOLPHINDB_INT128 = "INT128";
    private static final String DOLPHINDB_LONG = "LONG";
    private static final String DOLPHINDB_FLOAT = "FLOAT";
    private static final String DOLPHINDB_DOUBLE = "DOUBLE";
    private static final String DOLPHINDB_DECIMAL32 = "DECIMAL32(S)";
    private static final String DOLPHINDB_DECIMAL64 = "DECIMAL64(S)";
    private static final String DOLPHINDB_DECIMAL128 = "DECIMAL128(S)";

    // -------------------------string----------------------------
    private static final String DOLPHINDB_CHAR = "CHAR";
    private static final String DOLPHINDB_STRING = "STRING";
    private static final String DOLPHINDB_SYMBOL = "SYMBOL";

    // -------------------------date----------------------------
    private static final String DOLPHINDB_DATE = "DATE";
    private static final String DOLPHINDB_MONTH = "MONTH";
    private static final String DOLPHINDB_TIME = "TIME";
    private static final String DOLPHINDB_MINUTE = "MINUTE";
    private static final String DOLPHINDB_SECOND = "SECOND";
    private static final String DOLPHINDB_DATETIME = "DATETIME";
    private static final String DOLPHINDB_TIMESTAMP = "TIMESTAMP";
    private static final String DOLPHINDB_NANOTIME = "NANOTIME";
    private static final String DOLPHINDB_NANOTIMESTAMP = "NANOTIMESTAMP";
    private static final String DOLPHINDB_DATEHOUR = "DATEHOUR";

    // -------------------------not support table filed type-----------------------

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String dolphinDbType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        switch (dolphinDbType) {
            case DOLPHINDB_SHORT:
                return BasicType.SHORT_TYPE;
            case DOLPHINDB_INT:
            case DOLPHINDB_INT128:
                return BasicType.INT_TYPE;
            case DOLPHINDB_LONG:
                return BasicType.LONG_TYPE;
            case DOLPHINDB_FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOLPHINDB_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DOLPHINDB_DECIMAL32:
                return new DecimalType(precision, 9);
            case DOLPHINDB_DECIMAL64:
                return new DecimalType(precision, 18);
            case DOLPHINDB_DECIMAL128:
                return new DecimalType(precision, 38);
            case DOLPHINDB_CHAR:
            case DOLPHINDB_STRING:
            case DOLPHINDB_SYMBOL:
            case DOLPHINDB_UUID:
                return BasicType.STRING_TYPE;
            case DOLPHINDB_DATE:
            case DOLPHINDB_MONTH:
            case DOLPHINDB_DATEHOUR:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DOLPHINDB_DATETIME:
            case DOLPHINDB_TIMESTAMP:
            case DOLPHINDB_NANOTIME:
            case DOLPHINDB_NANOTIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DOLPHINDB_TIME:
            case DOLPHINDB_MINUTE:
            case DOLPHINDB_SECOND:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DOLPHINDB_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case DOLPHINDB_VOID:
                return BasicType.VOID_TYPE;
            case DOLPHINDB_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.GBASE_8A, dolphinDbType, jdbcColumnName);
        }
    }
}
