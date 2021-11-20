package com.tencent.cloud.oceanus.connector.file.split;

import org.apache.flink.api.connector.source.SourceSplit;

/** */
public class FileSourceSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
