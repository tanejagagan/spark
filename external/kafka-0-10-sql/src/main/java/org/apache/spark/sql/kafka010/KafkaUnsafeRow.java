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

package org.apache.spark.sql.kafka010;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;


import java.nio.ByteBuffer;

/*
 * Currently appending any number of fields to value is supported.
 * Key fields and fixed fields cannot be changed.
 */

public final class KafkaUnsafeRow extends InternalRow {

    public static final int FIXED_FIELDS = 4;
    public static final int TOPIC_FIELD_INDEX = 0 ;
    public static final int PARTITION_FIELD_INDEX = 1;
    public static final int OFFSET_FIELDS_INDEX =  2;
    public static final int TIMESTAMP_FIELDS_INDEX = 3;
    private final int numKeyFields;
    private final int partition;
    private final int keyFieldStart;
    private final int valueFieldStart;
    private final UTF8String topic;

    private InternalRow key;
    private InternalRow value;
    private int numValueFields;
    private boolean isKeyNull;
    private boolean isValueNull;
    private long offset;
    private long timestamp;

    public KafkaUnsafeRow(String topic, int partition, int numKeyFields) {
        this.numKeyFields = numKeyFields;
        this.partition = partition;
        this.valueFieldStart = FIXED_FIELDS + numKeyFields ;
        this.topic = UTF8String.fromString(topic);

        if (numKeyFields > 0) {
            key = new UnsafeRow(numKeyFields);
            keyFieldStart = FIXED_FIELDS;
        } else {
            key = null;
            isKeyNull = true;
            keyFieldStart = -1;
        }
        value = new UnsafeRow(0);
    }

    public void pointsTo(long offset, long timestamp,
                         boolean isKeyNull, ByteBuffer keyBuffer, boolean isValueNull, ByteBuffer valueBuffer) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.isKeyNull = isKeyNull;
        this.isValueNull = isValueNull;

        if (!isKeyNull && key != null) {
            key = UnsafeRow.readInternal(keyBuffer, (UnsafeRow) key);
        }
        if (!isValueNull && value != null) {
            value = UnsafeRow.readInternal(valueBuffer, (UnsafeRow) value);
            numValueFields = value.numFields();
        }
    }

    private boolean isValue(int ordinal) {
        return value != null && ordinal >= valueFieldStart
                && ordinal < valueFieldStart + value.numFields();
    }

    private boolean isKey(int ordinal) {
        return key != null && ordinal >= keyFieldStart && ordinal < valueFieldStart;
    }

    @Override
    public int numFields() {
        return FIXED_FIELDS + numKeyFields + numValueFields ;
    }

    @Override
    public void setNullAt(int i) {
        throw new IllegalArgumentException("Updates are not supported");
    }

    @Override
    public void update(int i, Object value) {
        throw new IllegalArgumentException("Updates are not supported");
    }

    @Override
    public InternalRow copy() {
        throw new IllegalArgumentException("Copy is not supported");
    }

    @Override
    public boolean isNullAt(int ordinal) {
        boolean res = true;
        if (ordinal < FIXED_FIELDS) {
            res = false;
        } else if (isValue(ordinal) && !isValueNull) {
            res = value.isNullAt(ordinal - valueFieldStart);
        } else if (isKey(ordinal) && !isKeyNull) {
            res = key.isNullAt(ordinal - keyFieldStart);
        }
        return res;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        if (isValue(ordinal)) {
            return value.getBoolean(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getBoolean(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public byte getByte(int ordinal) {
        if (isValue(ordinal)) {
            return value.getByte(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getByte(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public short getShort(int ordinal) {
        if (isValue(ordinal)) {
            return value.getShort(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getShort(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public int getInt(int ordinal) {
        int res ;
        if (isValue(ordinal)) {
            res = value.getInt(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            res = key.getInt(ordinal - keyFieldStart);
        } else if (ordinal == PARTITION_FIELD_INDEX) {
            res = partition;
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
        return res;
    }

    @Override
    public long getLong(int ordinal) {
        long res ;
        if (isValue(ordinal)) {
            res = value.getLong(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            res = key.getLong(ordinal - keyFieldStart);
        } else if (ordinal == OFFSET_FIELDS_INDEX) {
            res = offset;
        } else if (ordinal == TIMESTAMP_FIELDS_INDEX) {
            res = timestamp;
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
        return res;
    }

    @Override
    public float getFloat(int ordinal) {
        if (isValue(ordinal)) {
            return value.getFloat(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getFloat(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public double getDouble(int ordinal) {
        if (isValue(ordinal)) {
            return value.getDouble(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getDouble(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        if (isValue(ordinal)) {
            return value.getDecimal(ordinal - valueFieldStart, precision, scale);
        } else if (isKey(ordinal)) {
            return key.getDecimal(ordinal - keyFieldStart, precision, scale);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        UTF8String res  ;
        if (isValue(ordinal)) {
            res = value.getUTF8String(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            res = key.getUTF8String(ordinal - keyFieldStart);
        } else if(ordinal == TOPIC_FIELD_INDEX) {
            res = topic;
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
        return res ;
    }

    @Override
    public byte[] getBinary(int ordinal) {
        if (isValue(ordinal)) {
            return value.getBinary(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getBinary(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        if (isValue(ordinal)) {
            return value.getInterval(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getInterval(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
        if (isValue(ordinal)) {
            return value.getStruct(ordinal - valueFieldStart, numFields);
        } else if (isKey(ordinal)) {
            return key.getStruct(ordinal - keyFieldStart, numFields);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public ArrayData getArray(int ordinal) {
        if (isValue(ordinal)) {
            return value.getArray(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getArray(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public MapData getMap(int ordinal) {
        if (isValue(ordinal)) {
            return value.getMap(ordinal - valueFieldStart);
        } else if (isKey(ordinal)) {
            return key.getMap(ordinal - keyFieldStart);
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        if (isNullAt(ordinal)) {
            return null;
        }
        if (isValue(ordinal)) {
            return value.get(ordinal - valueFieldStart, dataType);
        } else if (isKey(ordinal)) {
            return key.get(ordinal - keyFieldStart, dataType);
        } else if (ordinal == 0) {
            return partition;
        } else if (ordinal == 1) {
            return offset;
        } else if (ordinal == 2) {
            return timestamp;
        } else {
            throw new RuntimeException("unexpected ordinal: " + ordinal);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
                .append(topic).append(",")
                .append(partition).append(",")
                .append(offset).append(",")
                .append(timestamp).append(",")
                .append("[").append(numKeyFields).append(",").append(isKeyNull? null : key.toString()).append("]").append(",")
                .append("[").append(numValueFields).append(",").append(isValueNull? null : value.toString()).append("]")
                .append("]");
        return sb.toString();
    }

    InternalRow getKey() {
        return key;
    }

    InternalRow getValue() {
        return value;
    }
}
