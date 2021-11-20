package com.tencent.cloud.oceanus.connector.file.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

/** */
public class FileSourceRecordEmitter
        implements RecordEmitter<RecordAndPosition, RowData, FileSourceSplitState> {
    @Override
    public void emitRecord(
            RecordAndPosition element,
            SourceOutput<RowData> output,
            FileSourceSplitState splitState)
            throws Exception {
        output.collect(element.getRecord());
        splitState.setOffset(element.getOffset());
    }
}
