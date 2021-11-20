package com.tencent.cloud.oceanus.connector.file.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import com.tencent.cloud.oceanus.connector.file.enumerator.FileSourceEnumerator;
import com.tencent.cloud.oceanus.connector.file.enumerator.PendingSplitsCheckpoint;
import com.tencent.cloud.oceanus.connector.file.enumerator.PendingSplitsCheckpointSerializer;
import com.tencent.cloud.oceanus.connector.file.reader.FileSourceReader;
import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;
import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplitSerializer;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileSource
        implements Source<RowData, FileSourceSplit, PendingSplitsCheckpoint>,
                ResultTypeQueryable<RowData> {
    private final TypeInformation<RowData> producedTypeInfo;
    private final Path[] paths;

    public FileSource(TypeInformation<RowData> producedTypeInfo, Path[] paths) {
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.paths = checkNotNull(paths);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, FileSourceSplit> createReader(SourceReaderContext context)
            throws Exception {
        return new FileSourceReader(context);
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileSourceSplit> context) throws Exception {
        return new FileSourceEnumerator(
                context, paths, Collections.emptyList(), Collections.emptyList());
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileSourceSplit> context, PendingSplitsCheckpoint checkpoint)
            throws Exception {
        return new FileSourceEnumerator(
                context, paths, checkpoint.getSplits(), checkpoint.getAlreadyProcessedPaths());
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
