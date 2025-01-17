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

package org.apache.shardingsphere.single.route;

import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.QualifiedTable;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.infra.route.type.DecorateSQLRouter;
import org.apache.shardingsphere.infra.route.type.EntranceSQLRouter;
import org.apache.shardingsphere.infra.route.type.TableSQLRouter;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.single.constant.SingleOrder;
import org.apache.shardingsphere.single.route.engine.SingleRouteEngineFactory;
import org.apache.shardingsphere.single.rule.SingleRule;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateTableStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Single SQL router.
 */
@HighFrequencyInvocation
public final class SingleSQLRouter implements EntranceSQLRouter<SingleRule>, DecorateSQLRouter<SingleRule>, TableSQLRouter<SingleRule> {
    
    @Override
    public RouteContext createRouteContext(final QueryContext queryContext, final RuleMetaData globalRuleMetaData, final ShardingSphereDatabase database,
                                           final SingleRule rule, final Collection<String> tableNames, final ConfigurationProperties props) {
        if (1 == database.getResourceMetaData().getStorageUnits().size()) {
            return createSingleDataSourceRouteContext(rule, database, queryContext);
        }
        RouteContext result = new RouteContext();
        Collection<QualifiedTable> singleTables = getSingleTables(database, rule, result, queryContext.getSqlStatementContext());
        SingleRouteEngineFactory.newInstance(singleTables, queryContext.getSqlStatementContext().getSqlStatement(), queryContext.getHintValueContext())
                .ifPresent(optional -> optional.route(result, rule));
        return result;
    }
    
    @Override
    public void decorateRouteContext(final RouteContext routeContext, final QueryContext queryContext, final ShardingSphereDatabase database,
                                     final SingleRule rule, final Collection<String> tableNames, final ConfigurationProperties props) {
        Collection<QualifiedTable> singleTables = getSingleTables(database, rule, routeContext, queryContext.getSqlStatementContext());
        SingleRouteEngineFactory.newInstance(singleTables, queryContext.getSqlStatementContext().getSqlStatement(), queryContext.getHintValueContext())
                .ifPresent(optional -> optional.route(routeContext, rule));
    }
    
    private RouteContext createSingleDataSourceRouteContext(final SingleRule rule, final ShardingSphereDatabase database, final QueryContext queryContext) {
        String logicDataSource = rule.getDataSourceNames().iterator().next();
        String actualDataSource = database.getResourceMetaData().getStorageUnits().keySet().iterator().next();
        RouteContext result = new RouteContext();
        Collection<String> tableNames = queryContext.getSqlStatementContext() instanceof TableAvailable
                ? ((TableAvailable) queryContext.getSqlStatementContext()).getTablesContext().getTableNames()
                : Collections.emptyList();
        result.getRouteUnits().add(new RouteUnit(new RouteMapper(logicDataSource, actualDataSource), createTableMappers(tableNames)));
        return result;
    }
    
    private Collection<RouteMapper> createTableMappers(final Collection<String> tableNames) {
        Collection<RouteMapper> result = new LinkedList<>();
        for (String each : tableNames) {
            result.add(new RouteMapper(each, each));
        }
        return result;
    }
    
    private Collection<QualifiedTable> getSingleTables(final ShardingSphereDatabase database, final SingleRule rule, final RouteContext routeContext, final SQLStatementContext sqlStatementContext) {
        Collection<QualifiedTable> qualifiedTables = rule.getQualifiedTables(sqlStatementContext, database);
        return routeContext.getRouteUnits().isEmpty() && sqlStatementContext.getSqlStatement() instanceof CreateTableStatement ? qualifiedTables : rule.getSingleTables(qualifiedTables);
    }
    
    @Override
    public int getOrder() {
        return SingleOrder.ORDER;
    }
    
    @Override
    public Class<SingleRule> getTypeClass() {
        return SingleRule.class;
    }
}
