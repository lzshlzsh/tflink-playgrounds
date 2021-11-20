package com.tencent.cloud.oceanus.connector.file.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileRecords implements RecordsWithSplitIds<RecordAndPosition> {
    @Nullable private String splitId;

    @Nullable private Iterator<RecordAndPosition> recordsForSplitCurrent;

    private final Iterator<RecordAndPosition> recordsForSplit;

    private final Set<String> finishedSplits;

    // ------------------------------------------------------------------------

    private FileRecords(
            @Nullable String splitId,
            Collection<RecordAndPosition> recordsForSplit,
            Set<String> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = checkNotNull(recordsForSplit).iterator();
        this.finishedSplits = checkNotNull(finishedSplits);
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

        return nextSplit;
    }

    @Nullable
    @Override
    public RecordAndPosition nextRecordFromSplit() {
        final Iterator<RecordAndPosition> recordsForSplit = this.recordsForSplitCurrent;
        if (recordsForSplit != null) {
            return recordsForSplit.hasNext() ? recordsForSplit.next() : null;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    // ------------------------------------------------------------------------

    public static FileRecords forRecords(
            final String splitId, Collection<RecordAndPosition> recordsForSplit) {
        return new FileRecords(splitId, recordsForSplit, Collections.emptySet());
    }

    public static FileRecords finishedSplit(String splitId) {
        return new FileRecords(null, Collections.emptySet(), Collections.singleton(splitId));
    }
}
