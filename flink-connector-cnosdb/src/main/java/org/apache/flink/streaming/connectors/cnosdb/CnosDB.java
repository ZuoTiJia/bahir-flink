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


import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Credentials;
import okhttp3.Response;
import okhttp3.Request;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;


public class CnosDB {
    OkHttpClient client;
    CnosDBConfig cnosDBConfig;

    private static final MediaType TEXT =
            MediaType.get("text/plain; charset=utf-8");

    private static final String TENANT_QUERY_KEY = "tenant";
    private static final String DATABASE_QUERY_KEY = "db";

    CnosDB(CnosDBConfig config) {
        this.client = new OkHttpClient();
        this.cnosDBConfig = config;
    }

    public void write(CnosDBPoint point) throws Exception{
        Preconditions.checkNotNull(point, "point can not be null");
        RequestBody body = RequestBody.create(point.lineProtocol(), TEXT);
        String urlString = Preconditions.checkNotNull(cnosDBConfig.getUrl(), "url can not be null");
        HttpUrl.Builder httpBuilder  = Preconditions.checkNotNull(HttpUrl.parse(urlString),  String.format("url:%s is invalid", urlString))
                .newBuilder();
        httpBuilder.addPathSegments("api/v1/write");

        String tenant = this.cnosDBConfig.getTenant();
        if (!StringUtils.isNullOrWhitespaceOnly(tenant)) {
            httpBuilder.addQueryParameter(TENANT_QUERY_KEY, tenant);
        }

        httpBuilder.addQueryParameter(DATABASE_QUERY_KEY, cnosDBConfig.getDatabase());
        String credential = Credentials.basic(cnosDBConfig.getUsername(), cnosDBConfig.getPassword());

        Request request = new Request.Builder().url(httpBuilder.build())
                .header("Authorization", credential)
                .post(body)
                .build();
        Response response = client.newCall(request).execute();
        if (response.code() != 200) {
            assert response.body() != null;
            throw new RuntimeException(response.body().string());
        }
    }

}
