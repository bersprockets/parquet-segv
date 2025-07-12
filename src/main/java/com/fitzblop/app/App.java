package com.fitzblop.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetOutputCommitter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        int arraySize = 230087527 + 1;
        int uniqueCount = 128*1024 + 1;
        String compressor = "NONE";

        if (args.length > 0) {
            arraySize = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            uniqueCount = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            compressor = args[2];
        }

        Job job = Job.getInstance(new Configuration());
        Configuration hadoopConf = ContextUtil.getConfiguration(job);
        Path outputPath = new Path("file:/tmp/bigarray_file");
        FileOutputFormat.setOutputPath(job, outputPath);
        Class outputCommitterClass = ParquetOutputCommitter.class;
        String outputCommitterClassName = outputCommitterClass.getCanonicalName();
        Class defaultClass = OutputCommitter.class;
        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job, MyWriteSupport.class);

        JobID jobId = new JobID("App", 1);
        TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
        TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);

        hadoopConf.set("mapreduce.job.id", jobId.toString());
        hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID().toString());
        hadoopConf.setBoolean("mapreduce.task.ismap", true);
        hadoopConf.setInt("mapreduce.task.partition", 0);
        hadoopConf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, ParquetOutputFormat.JobSummaryLevel.NONE);
        hadoopConf.set(ParquetOutputFormat.COMPRESSION, compressor);
        TaskAttemptContextImpl taskAttemptContext =
           new TaskAttemptContextImpl(hadoopConf, taskAttemptId);

        MyParquetOutputFormat pof = new MyParquetOutputFormat(outputPath);
        RecordWriter<Void, Row> recordWriter = pof.getRecordWriter(taskAttemptContext);

        long[] bigArray = new long[arraySize];
        for (int i = 0; i < arraySize; i++) {
            bigArray[i] = (i + 1) % uniqueCount;
        }
        Row row = new Row(bigArray);
        recordWriter.write(null, row);
        recordWriter.close(taskAttemptContext);
    }
}
