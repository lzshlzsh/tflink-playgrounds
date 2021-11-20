package com.tencent.cloud.oceanus.connector.file.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.tencent.cloud.oceanus.connector.file.source.FileSource;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileDynamicSource implements ScanTableSource {

    private final Path[] paths;
    private final ResolvedSchema schema;

    public FileDynamicSource(String path, ResolvedSchema schema) {
        this(Stream.of(checkNotNull(path)).map(Path::new).toArray(Path[]::new), schema);
    }

    public FileDynamicSource(Path[] paths, ResolvedSchema schema) {
        this.paths = checkNotNull(paths);
        this.schema = checkNotNull(schema);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(schema.toPhysicalRowDataType());

        return SourceProvider.of(new FileSource(producedTypeInfo, paths));
    }

    @Override
    public DynamicTableSource copy() {
        return new FileDynamicSource(paths, schema);
    }

    @Override
    public String asSummaryString() {
        return "FileSource";
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileDynamicSource that = (FileDynamicSource) o;
        return Arrays.equals(paths, that.paths) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(schema);
        result = 31 * result + Arrays.hashCode(paths);
        return result;
    }

    @Override
    public String toString() {
        return "FileDynamicSource{"
                + "paths="
                + Arrays.toString(paths)
                + ", schema="
                + schema
                + '}';
    }
}
