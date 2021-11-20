package com.tencent.cloud.oceanus.connector.file.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;
import com.tencent.cloud.oceanus.connector.file.split.PendingSplitsCheckpoint;

/** */
public class FileSource
        implements Source<RowData, FileSourceSplit, PendingSplitsCheckpoint>,
                ResultTypeQueryable<RowData> {
    private final TypeInformation<RowData> producedTypeInfo;

    public FileSource(TypeInformation<RowData> producedTypeInfo) {
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SourceReader<RowData, FileSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileSourceSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileSourceSplit> enumContext, PendingSplitsCheckpoint checkpoint)
            throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
