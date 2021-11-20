package com.tencent.cloud.oceanus.connector.file.reader;

import org.apache.flink.table.data.RowData;

/** */
public class RecordAndPosition {
    private final RowData record;
    private final long offset;

    public RecordAndPosition(RowData record, long offset) {
        this.record = record;
        this.offset = offset;
    }

    public RowData getRecord() {
        return record;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "RecordAndPosition{" + "record=" + record + ", offset=" + offset + '}';
    }
}
