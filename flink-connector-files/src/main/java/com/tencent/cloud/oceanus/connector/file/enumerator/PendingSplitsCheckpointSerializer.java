package com.tencent.cloud.oceanus.connector.file.enumerator;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class PendingSplitsCheckpointSerializer
        implements SimpleVersionedSerializer<PendingSplitsCheckpoint> {
    private static final int VERSION = 1;

    private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

    private final SimpleVersionedSerializer<FileSourceSplit> splitSerializer;

    public PendingSplitsCheckpointSerializer(
            SimpleVersionedSerializer<FileSourceSplit> splitSerializer) {
        this.splitSerializer = checkNotNull(splitSerializer);
    }

    // ------------------------------------------------------------------------
    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsCheckpoint checkpoint) throws IOException {
        checkArgument(
                checkpoint.getClass() == PendingSplitsCheckpoint.class,
                "Cannot serialize subclasses of PendingSplitsCheckpoint");

        // optimization: the splits lazily cache their own serialized form
        if (checkpoint.serializedFormCache != null) {
            return checkpoint.serializedFormCache;
        }

        final SimpleVersionedSerializer<FileSourceSplit> splitSerializer =
                this.splitSerializer; // stack cache
        final Collection<FileSourceSplit> splits = checkpoint.getSplits();
        final Collection<Path> processedPaths = checkpoint.getAlreadyProcessedPaths();

        final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
        final ArrayList<byte[]> serializedPaths = new ArrayList<>(processedPaths.size());

        int totalLen =
                16; // four ints: magic, version of split serializer, count splits, count paths

        for (FileSourceSplit split : splits) {
            final byte[] serSplit = splitSerializer.serialize(split);
            serializedSplits.add(serSplit);
            totalLen += serSplit.length + 4; // 4 bytes for the length field
        }

        for (Path path : processedPaths) {
            final byte[] serPath = path.toString().getBytes(StandardCharsets.UTF_8);
            serializedPaths.add(serPath);
            totalLen += serPath.length + 4; // 4 bytes for the length field
        }

        final byte[] result = new byte[totalLen];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
        byteBuffer.putInt(splitSerializer.getVersion());
        byteBuffer.putInt(serializedSplits.size());
        byteBuffer.putInt(serializedPaths.size());

        for (byte[] splitBytes : serializedSplits) {
            byteBuffer.putInt(splitBytes.length);
            byteBuffer.put(splitBytes);
        }

        for (byte[] pathBytes : serializedPaths) {
            byteBuffer.putInt(pathBytes.length);
            byteBuffer.put(pathBytes);
        }

        assert byteBuffer.remaining() == 0;

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        checkpoint.serializedFormCache = result;

        return result;
    }

    @Override
    public PendingSplitsCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        final int magic = bb.getInt();
        if (magic != VERSION_1_MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Invalid magic number for PendingSplitsCheckpoint. "
                                    + "Expected: %X , found %X",
                            VERSION_1_MAGIC_NUMBER, magic));
        }

        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numPaths = bb.getInt();

        final SimpleVersionedSerializer<FileSourceSplit> splitSerializer =
                this.splitSerializer; // stack cache
        final ArrayList<FileSourceSplit> splits = new ArrayList<>(numSplits);
        final ArrayList<Path> paths = new ArrayList<>(numPaths);

        for (int remaining = numSplits; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final FileSourceSplit split =
                    splitSerializer.deserialize(splitSerializerVersion, bytes);
            splits.add(split);
        }

        for (int remaining = numPaths; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final Path path = new Path(new String(bytes, StandardCharsets.UTF_8));
            paths.add(path);
        }

        return PendingSplitsCheckpoint.reusingCollection(splits, paths);
    }
}
