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

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class CnosDBPoint {
    private String measurement;
    private long timestamp;
    private Map<String, String> tags;
    private Map<String, Object> fields;

    private static final Function<String, String> FIELD_ESCAPER = s ->
            s.replace("\\", "\\\\").replace("\"", "\\\"");
    private static final Function<String, String> KEY_ESCAPER = s ->
            s.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=");

    private static final int MAX_FRACTION_DIGITS = 340;
    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
            ThreadLocal.withInitial(() -> {
                NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
                numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
                numberFormat.setGroupingUsed(false);
                numberFormat.setMinimumFractionDigits(1);
                return numberFormat;
            });

    public CnosDBPoint(String measurement, long timestamp, Map<String, String> tags, Map<String, Object> fields) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.tags = tags;
        this.fields = fields;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public void addTag(String key, String value) {
        if (this.tags == null) {
            this.tags = new HashMap<>();
        }
        this.tags.put(key, value);
    }

    public void addField(String key, Object value) {
        if (this.fields == null) {
            this.fields = new HashMap<>();
        }
        this.fields.put(key, value);
    }

    public String lineProtocol() {
        StringBuilder sb = new StringBuilder();
        sb.append(KEY_ESCAPER.apply(measurement));
        for (Map.Entry<String, String> tag: this.tags.entrySet()) {
            sb.append(',')
                    .append(KEY_ESCAPER.apply(tag.getKey()))
                    .append('=')
                    .append(KEY_ESCAPER.apply(tag.getValue()));
        }
        sb.append(' ');

        for (Map.Entry<String, Object> field : this.fields.entrySet()) {
            Object value = field.getValue();
            if (value == null) {
                continue;
            }

            sb.append(KEY_ESCAPER.apply(field.getKey())).append('=');
            if (value instanceof Number) {
                if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
                    sb.append(NUMBER_FORMATTER.get().format(value));
                } else {
                    sb.append(value).append('i');
                }
            } else if (value instanceof String) {
                String stringValue = (String) value;
                sb.append('"').append(FIELD_ESCAPER.apply(stringValue)).append('"');
            } else {
                sb.append(value);
            }

            sb.append(',');
        }

        int lengthMinusOne = sb.length() - 1;
        if (sb.charAt(lengthMinusOne) == ',') {
            sb.setLength(lengthMinusOne);
        }
        return sb.toString();
    }
}
