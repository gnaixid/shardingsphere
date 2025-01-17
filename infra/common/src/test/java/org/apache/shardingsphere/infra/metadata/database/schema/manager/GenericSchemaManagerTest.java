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

package org.apache.shardingsphere.infra.metadata.database.schema.manager;

import org.apache.shardingsphere.infra.database.core.DefaultDatabase;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.TableType;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GenericSchemaManagerTest {
    
    @Test
    void assertGetToBeAddedTablesBySchemas() {
        Map<String, ShardingSphereSchema> reloadSchemas = Collections.singletonMap("foo_schema",
                new ShardingSphereSchema(DefaultDatabase.LOGIC_NAME, Collections.singletonMap("foo_table", new ShardingSphereTable("foo_table",
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList())), Collections.emptyMap()));
        Map<String, ShardingSphereSchema> currentSchemas = Collections.singletonMap("foo_schema", new ShardingSphereSchema(DefaultDatabase.LOGIC_NAME, Collections.emptyMap(), Collections.emptyMap()));
        Map<String, ShardingSphereSchema> actual = GenericSchemaManager.getToBeAddedTablesBySchemas(reloadSchemas, currentSchemas);
        assertThat(actual.size(), is(1));
        assertThat(actual.get("foo_schema").getTables().size(), is(1));
        assertTrue(actual.get("foo_schema").getTables().containsKey("foo_table"));
    }
    
    @Test
    void assertGetToBeDroppedTablesBySchemas() {
        Map<String, ShardingSphereSchema> currentSchemas = Collections.singletonMap("foo_schema",
                new ShardingSphereSchema(DefaultDatabase.LOGIC_NAME, Collections.singletonMap("foo_table", new ShardingSphereTable("foo_table",
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList())), Collections.emptyMap()));
        Map<String, ShardingSphereSchema> reloadSchemas = Collections.singletonMap("foo_schema", new ShardingSphereSchema(DefaultDatabase.LOGIC_NAME, Collections.emptyMap(), Collections.emptyMap()));
        Map<String, ShardingSphereSchema> actual = GenericSchemaManager.getToBeDroppedTablesBySchemas(reloadSchemas, currentSchemas);
        assertThat(actual.size(), is(1));
        assertThat(actual.get("foo_schema").getTables().size(), is(1));
        assertTrue(actual.get("foo_schema").getTables().containsKey("foo_table"));
    }
    
    @Test
    void assertGetToBeAddedTables() {
        Map<String, ShardingSphereTable> actual = GenericSchemaManager.getToBeAddedTables(Collections.singletonMap("foo_table",
                new ShardingSphereTable("", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), TableType.TABLE)), Collections.emptyMap());
        assertThat(actual.size(), is(1));
        assertTrue(actual.containsKey("foo_table"));
    }
    
    @Test
    void assertGetToBeDroppedTables() {
        Map<String, ShardingSphereTable> actual = GenericSchemaManager.getToBeDroppedTables(Collections.emptyMap(), Collections.singletonMap("foo_table",
                new ShardingSphereTable("", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), TableType.TABLE)));
        assertThat(actual.size(), is(1));
        assertTrue(actual.containsKey("foo_table"));
    }
    
    @Test
    void assertGetToBeDroppedSchemas() {
        Map<String, ShardingSphereSchema> actual =
                GenericSchemaManager.getToBeDroppedSchemas(Collections.emptyMap(), Collections.singletonMap("foo_schema", new ShardingSphereSchema(DefaultDatabase.LOGIC_NAME)));
        assertThat(actual.size(), is(1));
        assertTrue(actual.containsKey("foo_schema"));
        
    }
}
