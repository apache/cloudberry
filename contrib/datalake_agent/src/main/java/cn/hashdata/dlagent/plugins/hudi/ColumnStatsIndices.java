/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.plugins.hudi.data.GenericRowData;
import cn.hashdata.dlagent.plugins.hudi.data.RowData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Utilities for abstracting away heavy-lifting of interactions with Metadata Table's Column Stats Index,
 * providing convenient interfaces to read it, transpose, etc.
 */
public class ColumnStatsIndices {
  // the column schema:
  // |- file_name: string
  // |- min_val: row
  // |- max_val: row
  // |- null_cnt: long
  // |- val_cnt: long
  // |- column_name: string
  private static final int ORD_FILE_NAME = 0;
  private static final int ORD_MIN_VAL = 1;
  private static final int ORD_MAX_VAL = 2;
  private static final int ORD_NULL_CNT = 3;
  private static final int ORD_VAL_CNT = 4;
  private static final int ORD_COL_NAME = 5;

  private ColumnStatsIndices() {
  }

  public static List<HoodieMetadataColumnStats> readColumnStatsIndex(Configuration conf,
                                                   String basePath,
                                                   HoodieMetadataConfig metadataConfig,
                                                   String[] targetColumns) {
    // NOTE: If specific columns have been provided, we can considerably trim down amount of data fetched
    //       by only fetching Column Stats Index records pertaining to the requested columns.
    //       Otherwise, we fall back to read whole Column Stats Index
    ValidationUtils.checkArgument(targetColumns.length > 0,
        "Column stats is only valid when push down filters have referenced columns");
    return readColumnStatsIndexByColumns(conf, basePath, targetColumns, metadataConfig);
  }

  /**
   * Transposes and converts the raw table format of the Column Stats Index representation,
   * where each row/record corresponds to individual (column, file) pair, into the table format
   * where each row corresponds to single file with statistic for individual columns collated
   * w/in such row:
   * <p>
   * Metadata Table Column Stats Index format:
   *
   * <pre>
   *  +---------------------------+------------+------------+------------+-------------+
   *  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
   *  +---------------------------+------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          A |          1 |         10 |           0 |
   *  | another_base_file.parquet |          A |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+------------+-------------+
   * </pre>
   * <p>
   * Returned table format
   *
   * <pre>
   *  +---------------------------+------------+------------+-------------+
   *  |          file             | A_minValue | A_maxValue | A_nullCount |
   *  +---------------------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          1 |         10 |           0 |
   *  | another_base_file.parquet |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+-------------+
   * </pre>
   * <p>
   * NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
   * query at hand might only be referencing a handful of those. As such, we collect all the
   * column references from the filtering expressions, and only transpose records corresponding to the
   * columns referenced in those
   *
   * @param colStats     RowData list bearing raw Column Stats Index table
   * @param queryColumns target columns to be included into the final table
   * @return reshaped table according to the format outlined above
   */
  public static Pair<List<RowData>, String[]> transposeColumnStatsIndex(List<HoodieMetadataColumnStats> colStats,
                                                                        String[] queryColumns) {
    // NOTE: We have to collect list of indexed columns to make sure we properly align the rows
    //       w/in the transposed dataset: since some files might not have all the columns indexed
    //       either due to the Column Stats Index config changes, schema evolution, etc. we have
    //       to make sure that all the rows w/in transposed data-frame are properly padded (with null
    //       values) for such file-column combinations
    Set<String> indexedColumns = colStats.stream().map(row -> row.getColumnName()
        .toString()).collect(Collectors.toSet());

    // NOTE: We're sorting the columns to make sure final index schema matches layout
    //       of the transposed table
    TreeSet<String> sortedTargetColumns = Arrays.stream(queryColumns).sorted()
        .filter(indexedColumns::contains)
        .collect(Collectors.toCollection(TreeSet::new));

    Map<String, List<HoodieMetadataColumnStats>> fileNameToRows = colStats.stream().parallel()
        .filter(row -> sortedTargetColumns.contains(row.getColumnName()))
        .collect(Collectors.groupingBy(row -> row.getFileName()));

    return Pair.of(foldRowsByFiles(sortedTargetColumns, fileNameToRows), sortedTargetColumns.toArray(new String[0]));
  }

  private static List<RowData> foldRowsByFiles(TreeSet<String> sortedTargetColumns,
                                               Map<String, List<HoodieMetadataColumnStats>> fileNameToRows) {
    return fileNameToRows.values().stream().parallel().map(rows -> {
      // Rows seq is always non-empty (otherwise it won't be grouped into)
      String fileName = rows.get(0).getFileName();
      long valueCount = rows.get(0).getValueCount();

      // To properly align individual rows (corresponding to a file) w/in the transposed projection, we need
      // to align existing column-stats for individual file with the list of expected ones for the
      // whole transposed projection (a superset of all files)
      Map<String, HoodieMetadataColumnStats> columnRowsMap = rows.stream()
              .collect(Collectors.toMap(row -> row.getColumnName(), row -> row));

      SortedMap<String, HoodieMetadataColumnStats> alignedColumnRowsMap = new TreeMap<>();
      sortedTargetColumns.forEach(col -> alignedColumnRowsMap.put(col, columnRowsMap.get(col)));

      List<Tuple3> columnStats = alignedColumnRowsMap.values().stream().map(row -> {
        if (row == null) {
          // NOTE: Since we're assuming missing column to essentially contain exclusively
          //       null values, we set null-count to be equal to value-count (this behavior is
          //       consistent with reading non-existent columns from Parquet)
          return Tuple3.of(null, null, valueCount);
        } else {
          return Tuple3.of(row.getMinValue(), row.getMaxValue(), row.getNullCount());
        }
      }).collect(Collectors.toList());

      GenericRowData foldedRow = new GenericRowData(2 + 3 * columnStats.size());
      foldedRow.setField(0, fileName);
      foldedRow.setField(1, valueCount);
      for (int i = 0; i < columnStats.size(); i++) {
        Tuple3 stats = columnStats.get(i);
        int startPos = 2 + 3 * i;
        foldedRow.setField(startPos, stats.f0);
        foldedRow.setField(startPos + 1, stats.f1);
        foldedRow.setField(startPos + 2, stats.f2);
      }
      return foldedRow;
    }).collect(Collectors.toList());
  }

  private static List<HoodieMetadataColumnStats> readColumnStatsIndexByColumns(
      Configuration conf,
      String basePath,
      String[] targetColumns,
      HoodieMetadataConfig metadataConfig) {

    HoodieTableMetadata metadataTable = HoodieTableMetadata.create(new HoodieLocalEngineContext(conf),
        metadataConfig, basePath,
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());

    List<String> encodedTargetColumnNames = Arrays.stream(targetColumns)
        .map(colName -> new ColumnIndexID(colName).asBase64EncodedString()).collect(Collectors.toList());

    HoodieData<HoodieRecord<HoodieMetadataPayload>> records =
        metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, false);

    return records
            .filter(record -> record.getData().getColumnStatMetadata().isPresent())
            .map(record -> record.getData().getColumnStatMetadata().get())
            .collectAsList();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private static class Tuple3 {
    public Object f0;
    public Object f1;
    public Object f2;

    private Tuple3(Object f0, Object f1, Object f2) {
      this.f0 = f0;
      this.f1 = f1;
      this.f2 = f2;
    }

    public static Tuple3 of(Object f0, Object f1, Object f2) {
      return new Tuple3(f0, f1, f2);
    }
  }
}