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

package org.apache.shardingsphere.infra.exception.generic;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.exception.core.external.sql.sqlstate.XOpenSQLState;
import org.apache.shardingsphere.infra.exception.core.external.sql.type.generic.GenericSQLException;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Unknown SQL exception.
 */
@Slf4j
public final class UnknownSQLException extends GenericSQLException {
    
    private static final long serialVersionUID = -7357918573504734977L;
    
    public UnknownSQLException(final Exception cause) {
        super("Unknown exception. Track:" + getStackTraceAsString(cause), XOpenSQLState.GENERAL_ERROR, 0, cause);
    }

    public static String getStackTraceAsString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        throwable.printStackTrace(printWriter);

        String fullStackTrace = stringWriter.toString();

        // 限制字符串长度为最多 10000 字符
        int maxLength = 10000;
        if (fullStackTrace.length() > maxLength) {
            return fullStackTrace.substring(0, maxLength) + "...(truncated) \n";
        } else {
            return fullStackTrace;
        }
    }
}
