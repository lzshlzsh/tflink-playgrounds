package com.tencent.cloud.oceanus.connector.file.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;

/** */
public class FileSourceSplitReader implements SplitReader<RecordAndPosition, FileSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSourceSplitReader.class);

    private final Queue<FileSourceSplit> splits;

    @Nullable private FSDataInputStream currentInStream;
    @Nullable private BufferedReader currentReader;
    @Nullable private String currentSplitId;

    // ------------------------------------------------------------------------

    public FileSourceSplitReader() {
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition> fetch() throws IOException {
        checkSplitOrStartNext();

        final String line = currentReader.readLine();

        if (line == null) {
            return finishSplit();
        }

        final GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString(line));
        final RecordAndPosition record = new RecordAndPosition(rowData, currentInStream.getPos());

        return FileRecords.forRecords(currentSplitId, Collections.singleton(record));
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FileSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    // ------------------------------------------------------------------------
    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final FileSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        final Path file = nextSplit.path();
        final long offset = nextSplit.getReaderPosition().orElse(nextSplit.offset());

        final FileSystem fs = file.getFileSystem();
        final long fileLength = fs.getFileStatus(file).getLen();

        if (offset < 0 || offset > fileLength) {
            throw new IOException(
                    String.format(
                            "Invalid offset for split %s, file length is %d",
                            nextSplit, fileLength));
        }
        currentInStream = fs.open(file);
        currentInStream.seek(offset);

        currentReader =
                new BufferedReader(new InputStreamReader(currentInStream, StandardCharsets.UTF_8));
        currentSplitId = nextSplit.splitId();
    }

    private FileRecords finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
            currentInStream = null;
        }

        final FileRecords finishRecords = FileRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }
}
