package ai.starlake.extract.impl.restapi

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.w3c.dom.{Document, Element, Node}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import javax.xml.parsers.DocumentBuilderFactory

/** Converts XML strings to Jackson JsonNode using standard JDK XML parsing. This avoids adding a
  * jackson-dataformat-xml dependency.
  *
  * Conversion rules:
  *   - XML elements become JSON object fields
  *   - Repeated sibling elements with the same name become JSON arrays
  *   - Text-only elements become JSON string values
  *   - Mixed content elements get a "#text" field for text content
  *   - XML attributes become fields prefixed with "@"
  */
object XmlToJsonConverter {

  def convert(xml: String, mapper: ObjectMapper): JsonNode = {
    val factory = DocumentBuilderFactory.newInstance()
    // Disable external entities for security
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false)
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
    val builder = factory.newDocumentBuilder()
    val doc: Document =
      builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
    doc.getDocumentElement.normalize()

    val root = doc.getDocumentElement
    val result = mapper.createObjectNode()
    result.set[ObjectNode](root.getTagName, elementToNode(root, mapper))
    result
  }

  private def elementToNode(element: Element, mapper: ObjectMapper): JsonNode = {
    val obj = mapper.createObjectNode()

    // Add attributes as @attr fields
    val attrs = element.getAttributes
    if (attrs != null) {
      for (i <- 0 until attrs.getLength) {
        val attr = attrs.item(i)
        obj.put(s"@${attr.getNodeName}", attr.getNodeValue)
      }
    }

    // Group child elements by tag name
    val children = element.getChildNodes
    val childElements = (0 until children.getLength)
      .map(children.item)
      .filter(_.getNodeType == Node.ELEMENT_NODE)
      .map(_.asInstanceOf[Element])

    val textContent = (0 until children.getLength)
      .map(children.item)
      .filter(_.getNodeType == Node.TEXT_NODE)
      .map(_.getTextContent.trim)
      .filter(_.nonEmpty)
      .mkString

    if (childElements.isEmpty) {
      // Leaf element — return as text value
      if (obj.isEmpty && attrs == null || attrs != null && attrs.getLength == 0) {
        return mapper.getNodeFactory.textNode(textContent)
      }
      // Has attributes + text
      if (textContent.nonEmpty) {
        obj.put("#text", textContent)
      }
      return obj
    }

    // Group children by tag name to detect arrays
    val grouped = childElements.groupBy(_.getTagName)
    grouped.foreach { case (tagName, elements) =>
      if (elements.size == 1) {
        obj.set[ObjectNode](tagName, elementToNode(elements.head, mapper))
      } else {
        val arr = mapper.createArrayNode()
        elements.foreach(e => arr.add(elementToNode(e, mapper)))
        obj.set[ObjectNode](tagName, arr)
      }
    }

    // If there's also text content mixed with elements
    if (textContent.nonEmpty) {
      obj.put("#text", textContent)
    }

    obj
  }
}
