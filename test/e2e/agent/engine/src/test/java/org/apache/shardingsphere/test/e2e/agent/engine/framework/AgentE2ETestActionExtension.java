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

package org.apache.shardingsphere.test.e2e.agent.engine.framework;

import org.apache.shardingsphere.test.e2e.agent.engine.env.AgentE2ETestEnvironment;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Agent E2E test action extension.
 */
public final class AgentE2ETestActionExtension implements BeforeAllCallback, AfterAllCallback {
    
    @Override
    public void beforeAll(final ExtensionContext extensionContext) {
        AgentE2ETestEnvironment.getInstance().init();
    }
    
    @Override
    public void afterAll(final ExtensionContext extensionContext) {
        AgentE2ETestEnvironment.getInstance().destroy();
    }
}