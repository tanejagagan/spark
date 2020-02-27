package org.apache.spark.sql.kafka010;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public final class KafkaProjectedInternalRow extends org.apache.spark.sql.catalyst.InternalRow {
    private final int[] projectionMap ;
    private InternalRow internal = null ;
    public KafkaProjectedInternalRow(int[] projectionMap ){
        this.projectionMap = projectionMap;
    }

    public void pointsTo(InternalRow row) {
        internal = row ;
    }

    @Override
    public int numFields() {
        return projectionMap.length;
    }

    @Override
    public void setNullAt( int i) {
        throwException(i);
    }

    @Override
    public void update( int i, Object value) {
        throwException(i);
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow copy() {
        return null;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        if(ordinal > projectionMap.length){
            return true ;
        }
        int i = projectionMap[ordinal];
        return internal.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getBoolean(i);
    }

    @Override
    public byte getByte(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getByte(i);
    }

    @Override
    public short getShort(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getShort(i);
    }

    @Override
    public int getInt(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getInt(i);
    }

    @Override
    public long getLong(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getLong(i);
    }

    @Override
    public float getFloat(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getFloat(i);
    }

    @Override
    public double getDouble(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getDouble(i);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        int i = projectionMap[ordinal];
        return internal.getDecimal(i, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getUTF8String(i);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getBinary(i);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getInterval(i);
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields) {
        int i = projectionMap[ordinal];
        return internal.getStruct(i, numFields);
    }

    @Override
    public ArrayData getArray(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getArray(i);
    }

    @Override
    public MapData getMap(int ordinal) {
        int i = projectionMap[ordinal];
        return internal.getMap(i);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        int i = projectionMap[ordinal];
        return  internal.get(i, dataType);
    }

    private void throwException(int ordinal) {
        throw new RuntimeException("Set operations are not supported");
    }
}
