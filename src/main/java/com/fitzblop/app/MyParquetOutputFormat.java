package com.fitzblop.app;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import java.io.IOException;

public class MyParquetOutputFormat extends ParquetOutputFormat<Row> {
  Path outputPath;

  public MyParquetOutputFormat(Path outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
    return outputPath;
  }
}
