package com.guizmaii.udemy.kafka.connect.github.data.utils

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder }

private[data] object SchemaBuilderOps {
  final val ARRAY_STRING_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA)

  final case class Field(key: String, schema: Schema)

  implicit final def asField(t: (String, Schema)): Field = Field(key = t._1, schema = t._2)

  /**
   * I was forced to create this wrapper because I wasn't able to enrich the Java `SchemaBuilder` class instances.
   *
   * I don't know why.
   */
  final class SchemaBuilderS(private val inner: SchemaBuilder) extends AnyVal {
    final def build: SchemaBuilder            = inner
    final def field(t: Field): SchemaBuilderS = new SchemaBuilderS(inner.field(t.key, t.schema))
  }

  final object SchemaBuilderS {
    final def apply(version: Int, name: String): SchemaBuilderS =
      new SchemaBuilderS(SchemaBuilder.struct().name(name).version(version))
  }
}
