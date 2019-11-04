package com.guizmaii.udemy.kafka.connect.github.data.utils

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder }

private[data] object SchemaBuilderOps {
  final val ARRAY_STRING_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA)

  /**
   * An ugly hack to force the compiler to typecheck the "fields". 😅
   */
  final case class Field(t: (String, Schema)) extends AnyVal
  implicit final def asField(t: (String, Schema)): Field = Field(t)

  /**
   * I was forced to create this wrapper because I wasn't able to enrich the Java `SchemaBuilder` class instances.
   *
   * I don't know why.
   */
  final class SchemaBuilderS(private val inner: SchemaBuilder) extends AnyVal {
    final def build: SchemaBuilder                = inner
    final def field(field: Field): SchemaBuilderS = new SchemaBuilderS(inner.field(field.t._1, field.t._2))
  }

  final object SchemaBuilderS {
    final def apply[T](version: Int, name: Class[T]): SchemaBuilderS =
      new SchemaBuilderS(SchemaBuilder.struct().name(name.getCanonicalName).version(version))
  }
}
