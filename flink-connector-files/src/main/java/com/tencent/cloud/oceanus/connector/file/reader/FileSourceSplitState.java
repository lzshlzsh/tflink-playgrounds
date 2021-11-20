package com.tencent.cloud.oceanus.connector.file.reader;

import org.apache.flink.util.FlinkRuntimeException;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileSourceSplitState {
    private final FileSourceSplit split;
    private @Nullable Long offset;

    // ------------------------------------------------------------------------

    public FileSourceSplitState(FileSourceSplit split) {
        this.split = checkNotNull(split);
        this.offset = split.getReaderPosition().orElse(null);
    }

    public @Nullable Long getOffset() {
        return offset;
    }

    public void setOffset(@Nullable Long offset) {
        // we skip sanity / boundary checks here for efficiency.
        // illegal boundaries will eventually be caught when constructing the split on checkpoint.
        this.offset = offset;
    }

    public FileSourceSplit toFileSourceSplit() {
        final FileSourceSplit updatedSplit = split.updateWithCheckpointedPosition(offset);

        // some sanity checks to avoid surprises and not accidentally lose split information
        if (updatedSplit == null) {
            throw new FlinkRuntimeException(
                    "Split returned 'null' in updateWithCheckpointedPosition(): " + split);
        }
        if (updatedSplit.getClass() != split.getClass()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Split returned different type in updateWithCheckpointedPosition(). "
                                    + "Split type is %s, returned type is %s",
                            split.getClass().getName(), updatedSplit.getClass().getName()));
        }

        return updatedSplit;
    }
}
