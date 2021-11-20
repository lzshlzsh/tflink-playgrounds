package com.tencent.cloud.oceanus.connector.file.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

/** FileSource connector Factory. */
public class FileDynamicTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "file";

    private static final ConfigOption<String> PATH =
            key("path").stringType().noDefaultValue().withDescription("The path of a directory");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();
        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final List<Column> physicalColumns =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());

        if (physicalColumns.size() != 1
                || !physicalColumns
                        .get(0)
                        .getDataType()
                        .getLogicalType()
                        .getTypeRoot()
                        .equals(LogicalTypeRoot.VARCHAR)) {
            throw new ValidationException(
                    String.format(
                            "Currently, we can only read files line by line. "
                                    + "That is, only one physical field of type STRING is supported, but got %d columns (%s).",
                            physicalColumns.size(),
                            physicalColumns.stream()
                                    .map(
                                            column ->
                                                    column.getName()
                                                            + " "
                                                            + column.getDataType().toString())
                                    .collect(Collectors.joining(", "))));
        }

        return new FileDynamicSource(config.get(PATH), schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
