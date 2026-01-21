/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.iceberg.flink.lor.example;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class LORSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(LORSink.class);

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    Configuration hadoopConf = new Configuration();

    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("uri", parameters.get("uri", "http://rest:8181"));
    catalogProperties.put("io-impl", parameters.get("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"));
    catalogProperties.put("warehouse", parameters.get("warehouse", "s3://warehouse/wh/"));
    catalogProperties.put("s3.endpoint", parameters.get("s3-endpoint", "http://s3.ozone:9878"));
    CatalogLoader catalogLoader = CatalogLoader.custom(
        "demo",
        catalogProperties,
        hadoopConf,
        parameters.get("catalog-impl", "org.apache.iceberg.rest.RESTCatalog"));
    Schema schema = new Schema(
        Types.NestedField.required(1, "character", Types.StringType.get()),
        Types.NestedField.required(2, "location", Types.StringType.get()),
        Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));
    Catalog catalog = catalogLoader.loadCatalog();
    String databaseName = parameters.get("database", "default");
    String tableName = parameters.getRequired("table");
    TableIdentifier outputTable = TableIdentifier.of(
        databaseName,
        tableName);
    if (!catalog.tableExists(outputTable)) {
      catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned());
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "10000")));

    FakerLORSource source = new FakerLORSource();
    source.setEventInterval(Float.parseFloat(parameters.get("event_interval", "5000")));
    DataStream<Row> stream = env.addSource(source)
        .returns(TypeInformation.of(Map.class)).map(s -> {
          Row row = new Row(3);
          row.setField(0, s.get("character"));
          row.setField(1, s.get("location"));
          row.setField(2, s.get("event_time"));
          return row;
        });
    // Configure row-based append
    FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
        .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
        .distributionMode(DistributionMode.HASH)
        .writeParallelism(2)
        .append();
    // Execute the flink app
    env.execute();
  }
}
