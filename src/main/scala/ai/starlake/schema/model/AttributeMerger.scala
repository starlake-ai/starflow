package ai.starlake.schema.model

import ai.starlake.schema.handlers.SchemaHandler
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import scala.annotation.tailrec

/** Attribute merging logic extracted from TableAttribute companion object.
  */
object AttributeMerger {

  /** Compare refAttr with sourceAttr and reject if emptiness of attributes is not the same.
    */
  private def checkAttributesEmptinessMismatch(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute
  ): Unit = {
    require(
      refAttr.attributes.isEmpty == sourceAttr.attributes.isEmpty,
      s"attribute with name ${sourceAttr.name} has mismatch on attributes emptiness"
    )
  }

  /** Compare refAttr with sourceAttr type and reject if containers (struct or array) doesn't match.
    * Furthermore, if it's an array type, check that nested element are the same container type as
    * well recursively.
    */
  private def checkContainerMismatch(refAttr: TableAttribute, sourceAttr: TableAttribute)(implicit
    schemaHandler: SchemaHandler
  ): Unit = {
    @tailrec
    def matchContainerType(refDataType: DataType, sourceDataType: DataType): Unit = {
      (refDataType, sourceDataType) match {
        case (_: ArrayType, _) | (_, _: ArrayType) =>
          require(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} is not in array in both schema"
          )
        case (_: StructType, _) =>
          require(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} found with type '${sourceAttr.`type`}' where type '${refAttr.`type`}' is expected"
          )
        case _ =>
      }
      (refDataType, sourceDataType) match {
        case (refArrayType: ArrayType, sourceArrayType: ArrayType) =>
          matchContainerType(refArrayType.elementType, sourceArrayType.elementType)
        case _ =>
      }
    }

    val refAttrDataType = refAttr.primitiveSparkType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSparkType(schemaHandler)
    matchContainerType(refAttrDataType, sourceAttrDataType)
  }

  private def merge(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute,
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): TableAttribute = {
    if (attributeMergeStrategy.failOnContainerMismatch) {
      checkContainerMismatch(refAttr, sourceAttr)
    }
    if (attributeMergeStrategy.failOnAttributesEmptinessMismatch) {
      checkAttributesEmptinessMismatch(refAttr, sourceAttr)
    }
    val refAttrDataType = refAttr.primitiveSparkType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSparkType(schemaHandler)
    val attributes = mergeContainerAttributes(
      refAttr,
      sourceAttr,
      attributeMergeStrategy,
      refAttrDataType,
      sourceAttrDataType
    )
    val (referenceSource, fallbackSource) =
      attributeMergeStrategy.attributePropertiesMergeStrategy match {
        case RefFirst    => refAttr    -> sourceAttr
        case SourceFirst => sourceAttr -> refAttr
      }
    refAttr.copy(
      privacy =
        if (
          referenceSource.privacy.isDefined && !referenceSource.privacy
            .contains(TransformInput.None)
        ) referenceSource.privacy
        else fallbackSource.privacy,
      comment = referenceSource.comment.orElse(fallbackSource.comment),
      rename = referenceSource.rename.orElse(fallbackSource.rename),
      metricType = referenceSource.metricType.orElse(fallbackSource.metricType),
      attributes = attributes,
      position = referenceSource.position.orElse(fallbackSource.position),
      default = referenceSource.default.orElse(fallbackSource.default),
      tags = if (referenceSource.tags.nonEmpty) referenceSource.tags else fallbackSource.tags,
      trim = referenceSource.trim.orElse(fallbackSource.trim),
      script = referenceSource.script.orElse(fallbackSource.script),
      foreignKey = referenceSource.foreignKey.orElse(fallbackSource.foreignKey),
      ignore = referenceSource.ignore.orElse(fallbackSource.ignore),
      accessPolicy = referenceSource.accessPolicy.orElse(fallbackSource.accessPolicy)
    )
  }

  private def mergeContainerAttributes(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute,
    attributeMergeStrategy: AttributeMergeStrategy,
    refAttrDataType: DataType,
    sourceAttrDataType: DataType
  )(implicit schemaHandler: SchemaHandler) = {
    val attributes = (refAttrDataType, sourceAttrDataType) match {
      case (_: StructType, _: StructType) =>
        mergeAll(refAttr.attributes, sourceAttr.attributes, attributeMergeStrategy)
      case (_: StructType, _) =>
        mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
      case (refType: ArrayType, sourceType: ArrayType) =>
        (refType.elementType, sourceType.elementType) match {
          case (_: StructType, _: StructType) =>
            mergeAll(refAttr.attributes, sourceAttr.attributes, attributeMergeStrategy)
          case (_: StructType, _) =>
            mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
          case _ =>
            Nil
        }
      case (refType: ArrayType, _) =>
        refType.elementType match {
          case _: StructType => mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
          case _             => Nil
        }
      case _ =>
        Nil
    }
    attributes
  }

  /** @param refAttrs
    *   attributes to enrich
    * @param sourceAttrs
    *   attributes used to enrich refAttrs
    * @return
    */
  def mergeAll(
    refAttrs: List[TableAttribute],
    sourceAttrs: List[TableAttribute],
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): List[TableAttribute] = {
    val missingAttributes = attributeMergeStrategy.keepSourceDiffAttributesStrategy match {
      case KeepAllDiff =>
        val missingAttributeNames = sourceAttrs.map(_.name).diff(refAttrs.map(_.name))
        missingAttributeNames.flatMap(attrName => sourceAttrs.find(_.name == attrName))
      case KeepOnlyScriptDiff =>
        val missingScriptAttributeNames =
          sourceAttrs.filter(_.script.isDefined).map(_.name).diff(refAttrs.map(_.name))
        missingScriptAttributeNames.flatMap(attrName => sourceAttrs.find(_.name == attrName))
      case DropAll => Nil
    }
    refAttrs.map { refAttr =>
      sourceAttrs.find(_.name == refAttr.name) match {
        case Some(sourceAttr) =>
          merge(refAttr, sourceAttr, attributeMergeStrategy)
        case None =>
          refAttr
      }
    } ++ missingAttributes
  }
}
