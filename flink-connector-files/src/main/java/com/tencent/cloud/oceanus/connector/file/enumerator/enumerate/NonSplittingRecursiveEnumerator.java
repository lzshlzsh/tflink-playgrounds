package com.tencent.cloud.oceanus.connector.file.enumerator.enumerate;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import com.tencent.cloud.oceanus.connector.file.split.FileSourceSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class NonSplittingRecursiveEnumerator implements FileEnumerator {

    private final char[] currentId = "0000000000000".toCharArray();

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        final ArrayList<FileSourceSplit> splits = new ArrayList<>();

        for (Path path : paths) {
            final FileSystem fs = path.getFileSystem();
            final FileStatus status = fs.getFileStatus(path);
            addSplitsForPath(status, fs, splits);
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    private void addSplitsForPath(
            FileStatus fileStatus, FileSystem fs, ArrayList<FileSourceSplit> target)
            throws IOException {
        if (!fileStatus.isDir()) {
            target.add(
                    new FileSourceSplit(getNextId(), fileStatus.getPath(), 0, fileStatus.getLen()));
            return;
        }

        final FileStatus[] containedFiles = fs.listStatus(fileStatus.getPath());
        for (FileStatus containedStatus : containedFiles) {
            addSplitsForPath(containedStatus, fs, target);
        }
    }

    protected final String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private static void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }
}
