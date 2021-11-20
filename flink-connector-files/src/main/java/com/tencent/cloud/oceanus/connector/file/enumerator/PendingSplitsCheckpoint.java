package com.tencent.cloud.oceanus.connector.file.enumerator;

import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.core.fs.Path;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class PendingSplitsCheckpoint {
    /** The splits in the checkpoint. */
    private final Collection<FileSourceSplit> splits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final Collection<Path> alreadyProcessedPaths;

    /**
     * The cached byte representation from the last serialization step. This helps to avoid paying
     * repeated serialization cost for the same checkpoint object. This field is used by {@link
     * PendingSplitsCheckpointSerializer}.
     */
    @Nullable byte[] serializedFormCache;

    protected PendingSplitsCheckpoint(
            Collection<FileSourceSplit> splits, Collection<Path> alreadyProcessedPaths) {
        this.splits = Collections.unmodifiableCollection(splits);
        this.alreadyProcessedPaths = Collections.unmodifiableCollection(alreadyProcessedPaths);
    }

    // ------------------------------------------------------------------------
    public Collection<FileSourceSplit> getSplits() {
        return splits;
    }

    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PendingSplitsCheckpoint{"
                + "splits="
                + splits
                + ", alreadyProcessedPaths="
                + alreadyProcessedPaths
                + '}';
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    public static PendingSplitsCheckpoint fromCollectionSnapshot(
            final Collection<FileSourceSplit> splits) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<FileSourceSplit> copy = new ArrayList<>(splits);
        return new PendingSplitsCheckpoint(copy, Collections.emptySet());
    }

    public static PendingSplitsCheckpoint fromCollectionSnapshot(
            final Collection<FileSourceSplit> splits,
            final Collection<Path> alreadyProcessedPaths) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<FileSourceSplit> splitsCopy = new ArrayList<>(splits);
        final Collection<Path> pathsCopy = new ArrayList<>(alreadyProcessedPaths);

        return new PendingSplitsCheckpoint(splitsCopy, pathsCopy);
    }

    static PendingSplitsCheckpoint reusingCollection(
            final Collection<FileSourceSplit> splits,
            final Collection<Path> alreadyProcessedPaths) {
        return new PendingSplitsCheckpoint(splits, alreadyProcessedPaths);
    }
}
