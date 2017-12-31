package com.waitingforcode.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

public class Filters {

    public static ColumnChunkMetaData getMetadataForColumn(BlockMetaData rowGroup, String columnName) {
        return rowGroup.getColumns().stream()
                .filter(columnChunkMetaData -> columnChunkMetaData.getPath().toDotString().contains(columnName))
                .findFirst().get();
    }

}
