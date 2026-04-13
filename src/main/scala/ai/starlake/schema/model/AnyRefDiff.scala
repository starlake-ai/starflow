package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonIgnore

import java.lang.reflect.Modifier
import java.util
import scala.jdk.CollectionConverters._

object AnyRefDiff {
  private def isComplexType(value: AnyRef): Boolean = value match {
    case null                             => false
    case _: String                        => false
    case _: java.lang.Number              => false
    case _: java.lang.Boolean             => false
    case _: java.lang.Enum[_]             => false
    case _: java.util.regex.Pattern       => false
    case _: scala.collection.Iterable[_]  => false
    case _: Option[_]                     => false
    case _: scala.Tuple1[_]               => false
    case _: scala.Tuple2[_, _]            => false
    case _: scala.Tuple3[_, _, _]         => false
    case p: Product if p.productArity > 0 => true
    case _                                => false
  }

  def extractFieldValues(obj: AnyRef): List[NamedValue] = {
    val cls = obj.getClass
    val fields = cls
      .getDeclaredFields()
      .filter(field => !Modifier.isTransient(field.getModifiers))
      .filter(field => !field.isAnnotationPresent(classOf[JsonIgnore]))
    val fieldNames = fields.map(_.getName)
    cls.getDeclaredMethods.flatMap { method =>
      if (
        fieldNames.contains(method.getName) &&
        Modifier.isPublic(method.getModifiers()) && method.getParameterCount == 0 && !method.getName
          .contains('$')
      ) {
        val invoked = method.invoke(obj)
        val result =
          if (invoked != null && invoked.isInstanceOf[Option[_]])
            invoked.asInstanceOf[Option[AnyRef]].getOrElse("")
          else
            invoked
        Some(NamedValue(method.getName, result))
      } else None
    }.toList
  }

  def diffListNamed(
    fieldName: String,
    existing: List[Named],
    incoming: List[Named]
  ): ListDiff[Named] = {
    val set1 = existing.toSet
    val set2 = incoming.toSet
    val deleted = Named.diff(set1, set2)
    val added = Named.diff(set2, set1)
    val common1 = Named.diff(set1, deleted).toList.sortBy(_.name)
    val common2 = Named.diff(set2, added).toList.sortBy(_.name)

    val common = common1.zip(common2).map { case (v1, v2) =>
      require(v1.name == v2.name)
      (v1.name, v1, v2)
    }
    val updated = common.flatMap { case (k, v1, v2) =>
      if (v1.isInstanceOf[NamedValue]) {
        val nv1 = v1.asInstanceOf[NamedValue]
        val nv2 = v2.asInstanceOf[NamedValue]
        if (v1 == v2) {
          Nil
        } else if (isComplexType(nv1.value) && isComplexType(nv2.value)) {
          val nested = diffAnyRef(nv1.name, nv1.value, nv2.value)
          nested.updated.map { case (parentOpt, v1, v2) =>
            val qualifiedName = parentOpt match {
              case Some(parent) => Some(s"${nv1.name}.$parent")
              case None         => Some(nv1.name)
            }
            (qualifiedName, v1, v2)
          }
        } else {
          List((Some(fieldName), v1, v2))
        }
      } else {
        val res = diffAnyRef(k, v1, v2)
        res.updated
      }
    }
    ListDiff(fieldName, added.toList, deleted.toList, updated)
  }

  def diffMap(
    fieldName: String,
    existing: Map[String, String],
    incoming: Map[String, String]
  ): ListDiff[Named] = {
    val existingNamed = existing.map { case (k, v) => NamedValue(k, v) }.toList
    val incomingNamed = incoming.map { case (k, v) => NamedValue(k, v) }.toList
    diffListNamed(fieldName, existingNamed, incomingNamed)
  }

  def diffAnyRef(
    fieldName: String,
    existing: AnyRef,
    incoming: AnyRef
  ): ListDiff[Named] = {
    val existingFields = extractFieldValues(existing)
    val incomingFields = extractFieldValues(incoming)
    diffListNamed(fieldName, existingFields, incomingFields)
  }

  def diffOptionAnyRef(
    fieldName: String,
    existing: Option[AnyRef],
    incoming: Option[AnyRef]
  ): ListDiff[Named] = {
    (existing, incoming) match {
      case (Some(existing), Some(incoming)) =>
        diffAnyRef(fieldName, existing, incoming)
      case (None, Some(incoming)) =>
        val incomingFields = extractFieldValues(incoming)
        diffListNamed(fieldName, Nil, incomingFields)
      case (Some(existing), None) =>
        val existingFields = extractFieldValues(existing)
        diffListNamed(fieldName, Nil, existingFields)
      case (None, None) =>
        diffListNamed(fieldName, Nil, Nil)
    }
  }

  def diffOptionString(
    fieldName: String,
    existing: Option[String],
    incoming: Option[String]
  ): ListDiff[String] = {
    diffSetString(fieldName, existing.toSet, incoming.toSet)
  }

  def diffSetString(
    fieldName: String,
    existing: Set[String],
    incoming: Set[String]
  ): ListDiff[String] = {
    val deleted = existing.diff(incoming)
    val added = incoming.diff(existing)
    ListDiff(fieldName, added.toList, deleted.toList, Nil)
  }

  /** @param existing
    * @param incoming
    * @return
    *   (added tables, deleted tables, common tables)
    */
  def partitionNamed[T <: Named](
    existing: List[T],
    incoming: List[T]
  ): (List[T], List[T], List[T]) = {
    val (commonTables, deletedTables) =
      existing.partition(table => incoming.map(_.name.toLowerCase).contains(table.name.toLowerCase))
    val addedTables =
      incoming.filter(table => !existing.map(_.name.toLowerCase).contains(table.name.toLowerCase))
    (addedTables, deletedTables, commonTables)
  }

}

case class AttributePropertyChange(key: String, before: String, after: String) {
  def getKey(): String = key
  def getBefore(): String = before
  def getAfter(): String = after
}

case class AttributeProperty[T](
  name: String,
  changes: List[(T, T)]
) {
  def getName(): String = name
  def getChanges(): util.List[AttributePropertyChange] = {
    changes.map { case (v1: NamedValue, v2: NamedValue) =>
      AttributePropertyChange(
        v1.getName(),
        Option(v1.getValue()).map(_.toString).getOrElse(""),
        Option(v2.getValue()).map(_.toString).getOrElse("")
      )
    }.asJava

  }
}

case class ListDiff[T](
  field: String,
  added: List[T],
  deleted: List[T],
  updated: List[(Option[String], T, T)]
) {
  @JsonIgnore
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.isEmpty

  def asOption(): Option[ListDiff[T]] = if (isEmpty()) None else Some(this)

  def getField(): String = field
  def getAdded(): java.util.List[T] = added.asJava
  def getDeleted(): java.util.List[T] = deleted.asJava

  def getUpdated(): util.List[AttributeProperty[T]] = {
    updated
      .groupBy(_._1)
      .map { case (n, kvs) =>
        val kv2s = kvs.map { case (_, v1, v2) =>
          (v1, v2)
        }
        AttributeProperty(n.getOrElse(""), kv2s)
      }
      .toList
      .asJava
  }

  def countAll() = added.size + deleted.size + updated.size

  def getCountAdded(): Int = added.size
  def getCountDeleted(): Int = deleted.size
  def getCountUpdated(): Int = updated.size
}

case class DomainsDiff(added: List[String], deleted: List[String], updated: List[DomainDiff]) {
  @JsonIgnore
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())

  def getAdded(): java.util.List[String] = added.asJava
  def getDeleted(): java.util.List[String] = deleted.asJava
  def getUpdated(): java.util.List[DomainDiff] = updated.asJava

  def getCountAdded(): Int = added.size
  def getCountDeleted(): Int = deleted.size
  def getCountUpdated(): Int = updated.size
}

case class JobsDiff(added: List[String], deleted: List[String], updated: List[TransformsDiff]) {
  @JsonIgnore
  def isEmpty(): Boolean =
    added.isEmpty && deleted.isEmpty && updated.forall(_.tasks.updated.isEmpty)
}

case class ProjectDiff(
  project1: String,
  project2: String,
  load: DomainsDiff,
  transform: JobsDiff
) {
  @JsonIgnore
  def isEmpty(): Boolean = load.isEmpty() && transform.isEmpty()

  def getProject1(): String = project1
  def getProject2(): String = project2
  def getLoad(): DomainsDiff = load
  def getTransform(): JobsDiff = transform

}
case class TableDiff(
  name: String,
  attributes: Option[ListDiff[Named]],
  pattern: Option[ListDiff[String]],
  metadata: Option[ListDiff[Named]],
  comment: Option[ListDiff[String]],
  presql: Option[ListDiff[String]],
  postsql: Option[ListDiff[String]],
  tags: Option[ListDiff[String]],
  rls: Option[ListDiff[Named]],
  expectations: Option[ListDiff[Named]],
  primaryKey: Option[ListDiff[String]],
  acl: Option[ListDiff[Named]],
  rename: Option[ListDiff[String]],
  sample: Option[ListDiff[String]],
  filter: Option[ListDiff[String]],
  patternSample: Option[ListDiff[String]]
) {
  @JsonIgnore
  def isEmpty(): Boolean =
    attributes.isEmpty &&
    expectations.isEmpty &&
    pattern.isEmpty &&
    metadata.isEmpty &&
    comment.isEmpty &&
    presql.isEmpty &&
    postsql.isEmpty &&
    tags.isEmpty &&
    rls.isEmpty &&
    primaryKey.isEmpty &&
    acl.isEmpty &&
    rename.isEmpty &&
    sample.isEmpty &&
    filter.isEmpty &&
    patternSample.isEmpty

  def getName(): String = name
  def getAttributes(): ListDiff[Named] = attributes.orNull
  def getPattern(): ListDiff[String] = pattern.orNull
  def getMetadata(): ListDiff[Named] = metadata.orNull
  def getComment(): ListDiff[String] = comment.orNull
  def getPresql(): ListDiff[String] = presql.orNull
  def getPostsql(): ListDiff[String] = postsql.orNull
  def getTags(): ListDiff[String] = tags.orNull
  def getRls(): ListDiff[Named] = rls.orNull
  def getExpectations(): ListDiff[Named] = expectations.orNull
  def getPrimaryKey(): ListDiff[String] = primaryKey.orNull
  def getAcl(): ListDiff[Named] = acl.orNull
  def getRename(): ListDiff[String] = rename.orNull
  def getSample(): ListDiff[String] = sample.orNull
  def getFilter(): ListDiff[String] = filter.orNull
  def getPatternSample(): ListDiff[String] = patternSample.orNull

  def hasAttributes(): Boolean = attributes.isDefined
  def hasPattern(): Boolean = pattern.isDefined
  def hasMetadata(): Boolean = metadata.isDefined
  def hasComment(): Boolean = comment.isDefined
  def hasPresql(): Boolean = presql.isDefined
  def hasPostsql(): Boolean = postsql.isDefined
  def hasTags(): Boolean = tags.isDefined
  def hasRls(): Boolean = rls.isDefined
  def hasExpectations(): Boolean = expectations.isDefined
  def hasPrimaryKey(): Boolean = primaryKey.isDefined
  def hasAcl(): Boolean = acl.isDefined
  def hasRename(): Boolean = rename.isDefined
  def hasSample(): Boolean = sample.isDefined
  def hasFilter(): Boolean = filter.isDefined
  def hasPatternSample(): Boolean = patternSample.isDefined

}

case class TablesDiff(added: List[String], deleted: List[String], updated: List[TableDiff]) {
  @JsonIgnore
  def isEmpty: Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())

  def getAdded(): java.util.List[String] = added.asJava
  def getDeleted(): java.util.List[String] = deleted.asJava
  def getUpdated(): java.util.List[TableDiff] = updated.asJava

  def getCountAdded(): Int = added.size
  def getCountDeleted(): Int = deleted.size
  def getCountUpdated(): Int = updated.size
}

case class DomainDiff(
  name: String,
  tables: TablesDiff,
  metadata: Option[ListDiff[Named]],
  comment: Option[ListDiff[String]],
  tags: Option[ListDiff[String]],
  rename: Option[ListDiff[String]],
  database: Option[ListDiff[String]]
) {
  @JsonIgnore
  def isEmpty(): Boolean =
    tables.isEmpty &&
    metadata.isEmpty &&
    comment.isEmpty &&
    tags.isEmpty &&
    rename.isEmpty &&
    database.isEmpty

  def getName(): String = name

  def getTables(): TablesDiff = tables
  def getMetadata(): ListDiff[Named] = metadata.orNull
  def getComment(): ListDiff[String] = comment.orNull
  def getTags(): ListDiff[String] = tags.orNull
  def getRename(): ListDiff[String] = rename.orNull
  def getDatabase(): ListDiff[String] = database.orNull

  def hasMetadata(): Boolean = metadata.isDefined
  def hasComment(): Boolean = comment.isDefined
  def hasTags(): Boolean = tags.isDefined
  def hasRename(): Boolean = rename.isDefined
  def hasDatabase(): Boolean = database.isDefined

}

case class TasksDiff(
  added: List[String],
  deleted: List[String],
  updated: List[ListDiff[Named]]
) {
  @JsonIgnore
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())

  def getAdded(): java.util.List[String] = added.asJava
  def getDeleted(): java.util.List[String] = deleted.asJava
  def getUpdated(): java.util.List[ListDiff[Named]] = updated.asJava

  def countAll() = added.size + deleted.size + updated.map(_.countAll()).sum
  def getCountAdded(): Int = added.size
  def getCountDeleted(): Int = deleted.size
  def getCountUpdated(): Int = updated.map(_.getCountUpdated()).sum

}

case class TransformsDiff(
  name: String,
  tasks: TasksDiff
) {
  @JsonIgnore
  def isEmpty(): Boolean = tasks.isEmpty()

  def getName(): String = name
  def getTasks(): TasksDiff = tasks
}
