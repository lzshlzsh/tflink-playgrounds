package com.tencent.cloud.oceanus.connector.file.enumerator.assigner;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

/** */
public class SimpleSplitAssigner implements FileSplitAssigner {
    private final ArrayList<FileSourceSplit> splits;

    public SimpleSplitAssigner(Collection<FileSourceSplit> splits) {
        this.splits = new ArrayList<>(splits);
    }

    // ------------------------------------------------------------------------

    @Override
    public Optional<FileSourceSplit> getNext() {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> newSplits) {
        splits.addAll(newSplits);
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return splits;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "SimpleSplitAssigner{" + "splits=" + splits + '}';
    }
}
