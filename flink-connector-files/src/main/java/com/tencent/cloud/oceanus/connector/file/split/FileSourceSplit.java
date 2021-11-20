package com.tencent.cloud.oceanus.connector.file.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    /** The unique ID of the split. Unique within the scope of this source. */
    private final String id;

    /** The path of the file referenced by this split. */
    private final Path filePath;

    /** The position of the first byte in the file to process. */
    private final long offset;

    /** The number of bytes in the file to process. */
    private final long length;

    /** The precise reader position in the split, to resume from. */
    @Nullable private final Long readerPosition;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link FileSourceSplitSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    // ------------------------------------------------------------------------

    public FileSourceSplit(String id, Path filePath, long offset, long length) {
        this(id, filePath, offset, length, null);
    }

    public FileSourceSplit(
            String id, Path filePath, long offset, long length, @Nullable Long readerPosition) {
        this(id, filePath, offset, length, readerPosition, null);
    }

    public FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            @Nullable Long readerPosition,
            @Nullable byte[] serializedForm) {
        checkArgument(offset >= 0, "offset must be >= 0");
        checkArgument(length >= 0, "length must be >= 0");

        this.id = checkNotNull(id);
        this.filePath = checkNotNull(filePath);
        this.offset = offset;
        this.length = length;
        this.readerPosition = readerPosition;
        this.serializedFormCache = serializedForm;
    }

    @Override
    public String splitId() {
        return id;
    }

    public Path path() {
        return filePath;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public Optional<Long> getReaderPosition() {
        return Optional.ofNullable(readerPosition);
    }

    public FileSourceSplit updateWithCheckpointedPosition(@Nullable Long position) {
        return new FileSourceSplit(id, filePath, offset, length, position);
    }

    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return "FileSourceSplit{"
                + "id='"
                + id
                + '\''
                + ", filePath="
                + filePath
                + ", offset="
                + offset
                + ", length="
                + length
                + ", readerPosition="
                + readerPosition
                + '}';
    }
}
