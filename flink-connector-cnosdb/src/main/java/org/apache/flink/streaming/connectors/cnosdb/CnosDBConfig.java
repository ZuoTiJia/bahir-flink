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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class CnosDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String url;

    private String username;

    private String password;

    private String tenant;

    private String database;

    public CnosDBConfig(CnosDBConfig.Builder builder) {
        Preconditions.checkArgument(builder != null, "CnosDBConfig builder can not be null");

        this.url = Preconditions.checkNotNull(builder.getUrl(), "host can not be null");
        this.username = Preconditions.checkNotNull(builder.getUsername(), "username can not be null");
        this.password = Preconditions.checkNotNull(builder.getPassword(), "password can not be null");
        this.tenant = builder.getTenant();
        this.database = Preconditions.checkNotNull(builder.getDatabase(), "database name can not be null");

    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private String username;
        private String password;

        private String tenant;
        private String database;

        public String getUrl() {
            return url;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getTenant() {
            return tenant;
        }

        public String getDatabase() {
            return database;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public CnosDBConfig build() {
            return new CnosDBConfig(this);
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTenant() {
        return tenant;
    }

    public String getDatabase() {
        return database;
    }
}
