/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.frontend.postgresql.command.query.extended.parse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLColumnType;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.parse.PostgreSQLComParsePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.parse.PostgreSQLParseCompletePacket;
import org.apache.shardingsphere.distsql.statement.DistSQLStatement;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.engine.SQLBindEngine;
import org.apache.shardingsphere.infra.exception.generic.UnknownSQLException;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.parser.SQLParserEngine;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.distsql.DistSQLStatementContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.query.extended.PostgreSQLServerPreparedStatement;
import org.apache.shardingsphere.sql.parser.sql.common.enums.ParameterMarkerType;
import org.apache.shardingsphere.sql.parser.sql.common.segment.SQLSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.ParameterMarkerSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.AbstractSQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.DMLStatement;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgreSQL command parse executor.
 */
@RequiredArgsConstructor
@Slf4j
public final class PostgreSQLComParseExecutor implements CommandExecutor {
    
    private final PostgreSQLComParsePacket packet;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public Collection<DatabasePacket> execute() {
        try {
            SQLParserEngine sqlParserEngine = createShardingSphereSQLParserEngine(connectionSession.getDatabaseName());
            String sql = packet.getSQL();

            SQLStatement sqlStatement = null;
            try {
                // 如果是DistSQL, 此处会成功解析为DistSQLStatement类型。
                sqlStatement = sqlParserEngine.parse(sql, true);
            } catch (Throwable e) {
                log.error("解析失败, 不支持的格式, 开始执行兼容处理. sql:{}", sql, e);
                sqlStatement = sqlParserEngine.parse("select 1", true);
            }

            HintValueContext hintValueContext = packet.getHintValueContext();
            Optional<String> hintDataSourceName = hintValueContext.findHintDataSourceName();

            //  如果不是探测型的SQL语句或DistSQL, 则应带上hint。
            //  没有hint则打印告警。(此处按照有没有成功解析出data_source_name来判断)
            if (!isWhiteListStatement(sql) && !(sqlStatement instanceof DistSQLStatement)) {
                if(!hintDataSourceName.isPresent()) {
                    log.warn("白名单外的SQL语句没有带Hint! 告警SQL:{}", sql);
                }
            }

//            // 测试UnknownSQLException名称长度. 结果: 200000个字符可以。
//            if(true) {
//                Exception exception = new Exception();
//
//                // 创建一个长的自定义堆栈跟踪
//                StackTraceElement[] longStackTrace = new StackTraceElement[5000];
//                for (int i = 0; i < longStackTrace.length; i++) {
//                    longStackTrace[i] = new StackTraceElement(
//                            "Class" + i, "method" + i, "File" + i + ".java", i);
//                }
//
//                // 设置异常的自定义堆栈跟踪
//                exception.setStackTrace(longStackTrace);
//
//                throw new UnknownSQLException(exception);
//            }

            String escapedSql = escape(sqlStatement, sql);
            if (!escapedSql.equalsIgnoreCase(sql)) {
                sqlStatement = sqlParserEngine.parse(escapedSql, true);
                sql = escapedSql;
            }

            List<Integer> actualParameterMarkerIndexes = new ArrayList<>();

//        if (sqlStatement.getParameterCount() > 0) {
//            List<ParameterMarkerSegment> parameterMarkerSegments = new ArrayList<>(((AbstractSQLStatement) sqlStatement).getParameterMarkerSegments());
//            for (ParameterMarkerSegment each : parameterMarkerSegments) {
//                actualParameterMarkerIndexes.add(each.getParameterIndex());
//            }
//            sql = convertSQLToJDBCStyle(parameterMarkerSegments, sql);
//            sqlStatement = sqlParserEngine.parse(sql, true);
//        }

            // 定义正则表达式来匹配 $ 后跟数字的模式
            Pattern pattern = Pattern.compile("\\$\\d+");
            Matcher matcher = pattern.matcher(sql);

            StringBuffer result = new StringBuffer();
            int index = 0;
            while (matcher.find()) {
                matcher.appendReplacement(result, "?");
                actualParameterMarkerIndexes.add(index);
                index++;
            }

            // 添加剩余的部分
            matcher.appendTail(result);
            sql = result.toString();

            List<PostgreSQLColumnType> paddedColumnTypes = paddingColumnTypes(sqlStatement.getParameterCount(), packet.readParameterTypes());
            SQLStatementContext sqlStatementContext = sqlStatement instanceof DistSQLStatement ? new DistSQLStatementContext((DistSQLStatement) sqlStatement)
                    : new SQLBindEngine(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData(), connectionSession.getDefaultDatabaseName(), packet.getHintValueContext())
                    .bind(sqlStatement, Collections.emptyList());
            PostgreSQLServerPreparedStatement serverPreparedStatement = new PostgreSQLServerPreparedStatement(sql, sqlStatementContext, packet.getHintValueContext(), paddedColumnTypes,
                    actualParameterMarkerIndexes);
            connectionSession.getServerPreparedStatementRegistry().addPreparedStatement(packet.getStatementId(), serverPreparedStatement);
            return Collections.singleton(PostgreSQLParseCompletePacket.getInstance());
        } catch (Throwable e) {
            log.error("PostgreSQLComParseExecutor.execute出现异常:{}", e.getMessage());
            throw e;
        }
    }

    public static boolean isWhiteListStatement(String sql) {
        // 去掉字符串前后的空格，并转换为小写以进行不区分大小写的匹配
        String trimmedSql = sql.trim().toLowerCase();

        // 使用正则表达式匹配符合条件的SQL语句
        return trimmedSql.matches("^set\\s+.*") ||
                trimmedSql.matches("^show\\s+.*") ||
                trimmedSql.matches("^select\\s+'x'\\s*") ||
                trimmedSql.matches("^select\\s+1\\s*") ||
                trimmedSql.matches(".*\\b(pg_\\w*|information_\\w*)\\b.*");
    }
    
    private SQLParserEngine createShardingSphereSQLParserEngine(final String databaseName) {
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return sqlParserRule.getSQLParserEngine(metaDataContexts.getMetaData().getDatabase(databaseName).getProtocolType());
    }
    
    private String escape(final SQLStatement sqlStatement, final String sql) {
        if (sqlStatement instanceof DMLStatement) {
            return sql.replace("?", "??");
        }
        return sql;
    }
    
    private String convertSQLToJDBCStyle(final List<ParameterMarkerSegment> parameterMarkerSegments, final String sql) {
        parameterMarkerSegments.sort(Comparator.comparingInt(SQLSegment::getStopIndex));
        StringBuilder result = new StringBuilder(sql);
        for (int i = parameterMarkerSegments.size() - 1; i >= 0; i--) {
            ParameterMarkerSegment each = parameterMarkerSegments.get(i);
            result.replace(each.getStartIndex(), each.getStopIndex() + 1, ParameterMarkerType.QUESTION.getMarker());
        }
        return result.toString();
    }
    
    private List<PostgreSQLColumnType> paddingColumnTypes(final int parameterCount, final List<PostgreSQLColumnType> specifiedColumnTypes) {
        if (parameterCount == specifiedColumnTypes.size()) {
            return specifiedColumnTypes;
        }
        List<PostgreSQLColumnType> result = new ArrayList<>(parameterCount);
        result.addAll(specifiedColumnTypes);
        int unspecifiedCount = parameterCount - specifiedColumnTypes.size();
        for (int i = 0; i < unspecifiedCount; i++) {
            result.add(PostgreSQLColumnType.UNSPECIFIED);
        }
        return result;
    }
}
