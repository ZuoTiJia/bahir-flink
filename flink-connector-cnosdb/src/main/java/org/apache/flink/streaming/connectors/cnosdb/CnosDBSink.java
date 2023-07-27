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

package org.apache.flink.streaming.connectors.cnosdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

public class CnosDBSink extends RichSinkFunction<CnosDBPoint> {
    private transient CnosDB cnosDBClient;
    private final CnosDBConfig cnosDBConfig;

    public CnosDBSink(CnosDBConfig cnosDBConfig) {
        this.cnosDBConfig = Preconditions.checkNotNull(cnosDBConfig, "CnosDB client config should not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cnosDBClient = new CnosDB(this.cnosDBConfig);
        super.open(parameters);
    }

    @Override
    public void invoke(CnosDBPoint value, Context context) throws Exception {

        if (StringUtils.isNullOrWhitespaceOnly(value.getMeasurement())) {
            throw new RuntimeException("No measurement defined");
        }
        cnosDBClient.write(value);
    }

    @Override
    public void close() throws Exception {
    }
}
