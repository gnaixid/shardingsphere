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

package org.apache.shardingsphere.test.e2e.env.runtime;

import com.google.common.base.Splitter;
import lombok.Getter;
import org.apache.shardingsphere.test.e2e.env.runtime.cluster.ClusterEnvironment;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioCommonPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.TimeZone;

/**
 * E2E test environment.
 */
@Getter
public final class E2ETestEnvironment {
    
    private static final E2ETestEnvironment INSTANCE = new E2ETestEnvironment();
    
    private final Collection<String> runModes;
    
    private final boolean runAdditionalTestCases;
    
    private final Collection<String> scenarios;
    
    private final ClusterEnvironment clusterEnvironment;
    
    private final boolean smoke;
    
    private final String nativeStorageHost;
    
    private final String nativeStoragePort;
    
    private final String nativeStorageUsername;
    
    private final String nativeStoragePassword;
    
    private E2ETestEnvironment() {
        Properties props = loadProperties();
        runModes = Splitter.on(",").trimResults().splitToList(props.getProperty("it.run.modes"));
        runAdditionalTestCases = Boolean.parseBoolean(props.getProperty("it.run.additional.cases"));
        TimeZone.setDefault(TimeZone.getTimeZone(props.getProperty("it.timezone", "UTC")));
        scenarios = getScenarios(props);
        smoke = Boolean.parseBoolean(props.getProperty("it.run.smoke"));
        clusterEnvironment = new ClusterEnvironment(props);
        nativeStorageHost = props.getProperty("it.native.storage.host");
        nativeStoragePort = props.getProperty("it.native.storage.port");
        nativeStorageUsername = props.getProperty("it.native.storage.username");
        nativeStoragePassword = props.getProperty("it.native.storage.password");
    }
    
    @SuppressWarnings("AccessOfSystemProperties")
    private Properties loadProperties() {
        Properties result = new Properties();
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("env/it-env.properties")) {
            result.load(inputStream);
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
        for (String each : System.getProperties().stringPropertyNames()) {
            result.setProperty(each, System.getProperty(each));
        }
        return result;
    }
    
    private Collection<String> getScenarios(final Properties props) {
        Collection<String> result = Splitter.on(",").trimResults().splitToList(props.getProperty("it.scenarios"));
        for (String each : result) {
            new ScenarioCommonPath(each).checkFolderExist();
        }
        return result;
    }
    
    /**
     * Get instance.
     *
     * @return singleton instance
     */
    public static E2ETestEnvironment getInstance() {
        return INSTANCE;
    }
}
