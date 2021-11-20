package com.tencent.cloud.oceanus.connector.file.split;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FileSourceSplitSerializer implements SimpleVersionedSerializer<FileSourceSplit> {
    public static final FileSourceSplitSerializer INSTANCE = new FileSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 1;

    // ------------------------------------------------------------------------

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FileSourceSplit split) throws IOException {
        checkArgument(
                split.getClass() == FileSourceSplit.class,
                "Cannot serialize subclasses of FileSourceSplit");

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeUTF(split.splitId());
        split.path().write(out);
        out.writeLong(split.offset());
        out.writeLong(split.length());

        final Optional<Long> readerPosition = split.getReaderPosition();
        out.writeBoolean(readerPosition.isPresent());
        if (readerPosition.isPresent()) {
            out.writeLong(readerPosition.get());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public FileSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final String id = in.readUTF();
        final Path path = new Path();
        path.read(in);
        final long offset = in.readLong();
        final long len = in.readLong();

        final Long readerPosition = in.readBoolean() ? in.readLong() : null;

        // instantiate a new split and cache the serialized form
        return new FileSourceSplit(id, path, offset, len, readerPosition, serialized);
    }
}
