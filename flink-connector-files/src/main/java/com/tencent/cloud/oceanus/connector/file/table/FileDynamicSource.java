package com.tencent.cloud.oceanus.connector.file.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.tencent.cloud.oceanus.connector.file.source.FileSource;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileDynamicSource implements ScanTableSource {

    private final String path;
    private final ResolvedSchema schema;

    public FileDynamicSource(String path, ResolvedSchema schema) {
        this.path = checkNotNull(path);
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
        return SourceProvider.of(new FileSource(producedTypeInfo));
    }

    @Override
    public DynamicTableSource copy() {
        return new FileDynamicSource(path, schema);
    }

    @Override
    public String asSummaryString() {
        return "FileSource";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileDynamicSource that = (FileDynamicSource) o;
        return Objects.equals(path, that.path) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, schema);
    }

    @Override
    public String toString() {
        return "FileDynamicSource{" + "path='" + path + '\'' + ", schema=" + schema + '}';
    }
}
