package com.fitzblop.app;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class MyWriteSupport extends WriteSupport<Row> {
  private static String repeatedGroupName = "list";
  private static String elementFieldName = "element";

  public MyWriteSupport() {}

  private RecordConsumer recordConsumer;

  public WriteContext init(Configuration var1) {
    Type elementType = Types.primitive(PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).named(elementFieldName);
    Type fieldType = Types
       .buildGroup(Type.Repetition.OPTIONAL).as(LogicalTypeAnnotation.listType())
       .addField(Types
          .repeatedGroup()
          .addField(elementType)
          .named(repeatedGroupName))
       .named("bigarray");

    MessageType messageType = Types
       .buildMessage()
       .addField(fieldType)
       .named("segv_schema");

    System.out.println("Message type is\n" + messageType);

    return new WriteContext(messageType, new HashMap<>());
  }

  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  public void write(Row row) {
    recordConsumer.startMessage();
    long[] array = row.getData();
    recordConsumer.startField("bigarray", 0);
    recordConsumer.startGroup();
    if (array.length > 0) {
      recordConsumer.startField(repeatedGroupName, 0);
      for (int i = 0; i < array.length; i++) {
        recordConsumer.startGroup();
        recordConsumer.startField(elementFieldName, 0);
        recordConsumer.addLong(array[i]);
        recordConsumer.endField(elementFieldName, 0);
        recordConsumer.endGroup();
      }
      recordConsumer.endField(repeatedGroupName, 0);
    }
    recordConsumer.endGroup();
    recordConsumer.endField("bigarray", 0);
    recordConsumer.endMessage();
  }
}
