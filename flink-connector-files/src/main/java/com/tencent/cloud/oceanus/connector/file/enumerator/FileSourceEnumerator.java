package com.tencent.cloud.oceanus.connector.file.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;

import com.tencent.cloud.oceanus.connector.file.enumerator.assigner.FileSplitAssigner;
import com.tencent.cloud.oceanus.connector.file.enumerator.assigner.SimpleSplitAssigner;
import com.tencent.cloud.oceanus.connector.file.enumerator.enumerate.FileEnumerator;
import com.tencent.cloud.oceanus.connector.file.enumerator.enumerate.NonSplittingRecursiveEnumerator;
import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileSourceEnumerator
        implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSourceEnumerator.class);

    private final FileEnumerator enumerator;
    private final FileSplitAssigner splitAssigner;

    private final SplitEnumeratorContext<FileSourceSplit> context;
    private final Path[] paths;
    private final HashSet<Path> alreadyDiscoveredPaths;

    private final LinkedHashMap<Integer, String> readersAwaitingSplit;
    // ------------------------------------------------------------------------

    public FileSourceEnumerator(
            SplitEnumeratorContext<FileSourceSplit> context,
            Path[] paths,
            Collection<FileSourceSplit> splits,
            Collection<Path> alreadyDiscoveredPaths) {
        this.enumerator = new NonSplittingRecursiveEnumerator();
        this.splitAssigner = new SimpleSplitAssigner(checkNotNull(splits));
        this.context = checkNotNull(context);
        this.paths = checkNotNull(paths);
        this.alreadyDiscoveredPaths = new HashSet<>(checkNotNull(alreadyDiscoveredPaths));
        this.readersAwaitingSplit = new LinkedHashMap<>();
    }

    @Override
    public void start() {
        context.callAsync(
                () -> enumerator.enumerateSplits(paths, 1),
                this::processDiscoveredSplits,
                2000,
                1000);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.put(subtaskId, requesterHostname);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        LOG.info("File Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        final PendingSplitsCheckpoint checkpoint =
                PendingSplitsCheckpoint.fromCollectionSnapshot(
                        splitAssigner.remainingSplits(), alreadyDiscoveredPaths);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    // ------------------------------------------------------------------------

    private void processDiscoveredSplits(Collection<FileSourceSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }

        final Collection<FileSourceSplit> newSplits =
                splits.stream()
                        .filter((split) -> alreadyDiscoveredPaths.add(split.path()))
                        .collect(Collectors.toList());
        splitAssigner.addSplits(newSplits);

        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
                readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }

            final String hostname = nextAwaiting.getValue();
            final int awaitingSubtask = nextAwaiting.getKey();
            final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext();
            if (nextSplit.isPresent()) {
                context.assignSplit(nextSplit.get(), awaitingSubtask);
                awaitingReader.remove();
            } else {
                break;
            }
        }
    }
}
