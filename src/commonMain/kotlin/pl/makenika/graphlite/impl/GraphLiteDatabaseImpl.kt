/*
 * Copyright (c) 2020 Mateusz Armatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.makenika.graphlite.impl

import com.benasher44.uuid.uuid4
import pl.makenika.graphlite.*
import pl.makenika.graphlite.impl.GraphSqlUtils.getElementId
import pl.makenika.graphlite.impl.GraphSqlUtils.getElementName
import pl.makenika.graphlite.impl.GraphSqlUtils.getFieldId
import pl.makenika.graphlite.impl.GraphSqlUtils.getSchemaFields
import pl.makenika.graphlite.sql.*

internal class GraphLiteDatabaseImpl internal constructor(private val driver: SqliteDriver) :
    GraphLiteDatabase {
    private val queryEngine by lazy { QueryEngine(driver) }

    override fun close() {
        driver.close()
    }

    override fun connect(edgeName: String, nodeName: String, outgoing: Boolean?): Connection {
        val connId = uuid4()
        val values = SqlContentValues().apply {
            put("id", connId.toString())
            put("edgeName", edgeName)
            put("nodeName", nodeName)
            put("outgoing", outgoing?.let { if (it) 1 else 0 })
        }
        driver.insertOrAbortAndThrow("Connection", values)
        return Connection(ConnectionIdImpl(connId.toString()), edgeName, nodeName, outgoing)
    }

    override fun connect(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection> {
        require(sourceNodeName != targetNodeName) { "Cannot connect the same node name=$sourceNodeName." }
        return driver.transaction {
            val a = connect(edgeName, sourceNodeName, if (directed) true else null)
            val b = connect(edgeName, targetNodeName, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun connectOrReplace(
        edgeName: String,
        nodeName: String,
        outgoing: Boolean?
    ): Connection {
        return transaction {
            disconnect(edgeName, nodeName)
            connect(edgeName, nodeName, outgoing)
        }
    }

    override fun connectOrReplace(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection> {
        return transaction {
            val a = connectOrReplace(edgeName, sourceNodeName, if (directed) true else null)
            val b = connectOrReplace(edgeName, targetNodeName, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun disconnect(edgeName: String, nodeName: String): Boolean {
        return driver.delete(
            "Connection",
            "edgeName = ? AND nodeName = ?",
            arrayOf(edgeName, nodeName)
        )
    }

    override fun getConnections(elementName: String): Sequence<Connection> {
        return sequence {
            driver.beginTransaction()
            try {
                driver.query(
                    "SELECT * FROM Connection WHERE edgeName = ? OR nodeName = ?", arrayOf(
                        elementName,
                        elementName
                    )
                ).use { cursor ->
                    while (cursor.moveToNext()) {
                        val connId = cursor.getString("id")
                        val edgeName = cursor.getString("edgeName")
                        val nodeName = cursor.getString("nodeName")
                        val outgoing = cursor.findBoolean("outgoing")
                        yield(Connection(ConnectionIdImpl(connId), edgeName, nodeName, outgoing))
                    }
                }
                driver.setTransactionSuccessful()
            } finally {
                driver.endTransaction()
            }
        }
    }

    override fun getOrConnect(edgeName: String, nodeName: String, outgoing: Boolean?): Connection {
        return transaction {
            findConnection(edgeName, nodeName) ?: connect(edgeName, nodeName, outgoing)
        }
    }

    override fun getOrConnect(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection> {
        return transaction {
            val a = getOrConnect(edgeName, sourceNodeName, if (directed) true else null)
            val b = getOrConnect(edgeName, targetNodeName, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun findConnection(edgeName: String, nodeName: String): Connection? {
        val bindings = arrayOf(edgeName, nodeName)
        return driver.transaction {
            driver.query("SELECT * FROM Connection WHERE edgeName = ? AND nodeName = ?", bindings)
                .use { cursor ->
                    if (cursor.moveToNext()) {
                        val connId = cursor.getString("id")
                        val eName = cursor.getString("edgeName")
                        val nName = cursor.getString("nodeName")
                        val outgoing = cursor.findBoolean("outgoing")
                        Connection(ConnectionIdImpl(connId), eName, nName, outgoing)
                    } else {
                        null
                    }
                }
        }
    }

    override fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S> {
        val id = EdgeIdImpl(uuid4().toString())
        val name = uuid4().toString()
        driver.transaction {
            createElement(fieldMap, id, name, ElementType.Edge)
        }
        return Edge(id, name, fieldMap)
    }

    override fun <S : Schema> createEdge(name: String, fieldMap: FieldMap<S>): Edge<S>? {
        val id = EdgeIdImpl(uuid4().toString())
        val ok = driver.transactionWithRollback {
            createElement(fieldMap, id, name, ElementType.Edge)
            true
        }.getOrDefault(false)
        return if (ok) {
            Edge(id, name, fieldMap)
        } else {
            null
        }
    }

    override fun <S : Schema> createOrReplaceEdge(name: String, fieldMap: FieldMap<S>): Edge<S> {
        val id = EdgeIdImpl(uuid4().toString())
        return driver.transaction {
            val oldElementId = getElementId(driver, name)
            if (oldElementId != null) {
                val oldSchema = findElementSchema(oldElementId)
                    ?: error("Could not find schema for edge name=$name")
                deleteFieldValues(oldElementId, oldSchema)
                createElement(fieldMap, id, name, ElementType.Edge, updateOrReplace = true)
            } else {
                createElement(fieldMap, id, name, ElementType.Edge)
            }
            Edge(id, name, fieldMap)
        }
    }

    override fun deleteEdge(name: String, withConnections: Boolean): Boolean {
        return deleteElementByName(name, withConnections)
    }

    override fun deleteEdge(id: EdgeId, withConnections: Boolean): Boolean {
        return deleteElementById(id, withConnections)
    }

    override fun findEdgeSchema(id: EdgeId): Schema? {
        return findElementSchema(id)
    }

    override fun <S : Schema> getOrCreateEdge(name: String, fieldMap: FieldMap<S>): Edge<S> {
        return driver.transaction {
            query(EdgeMatch(fieldMap.schema(), Where.name(name))).firstOrNull() ?: run {
                val id = EdgeIdImpl(uuid4().toString())
                createElement(fieldMap, id, name, ElementType.Edge)
                Edge(id, name, fieldMap)
            }
        }
    }

    override fun <S : Schema, T> updateField(edge: Edge<S>, field: Field<S, T>, value: T): Edge<S> {
        return driver.transaction {
            updateFieldValue(edge.id, edge.fieldMap.schema(), field, value)
            query(EdgeMatch(edge.fieldMap.schema(), Where.id(edge.id))).first()
        }
    }

    override fun <S : Schema, T> updateField(
        edge: Edge<S>,
        field: Field<S, T>,
        value: (T) -> T
    ): Edge<S> {
        return updateField(edge, field, value(edge[field]))
    }

    override fun <S : Schema> updateFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S> {
        return driver.transaction {
            val updatedId = updateFieldValues(
                edge.name,
                edge.id,
                edge.fieldMap.schema(),
                fieldMap
            ) { EdgeIdImpl(uuid4().toString()) }
            Edge(updatedId, edge.name, fieldMap)
        }
    }

    override fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S> {
        val id = NodeIdImpl(uuid4().toString())
        val name = uuid4().toString()
        driver.transaction {
            createElement(fieldMap, id, name, ElementType.Node)
        }
        return Node(id, name, fieldMap)
    }

    override fun <S : Schema> createNode(name: String, fieldMap: FieldMap<S>): Node<S>? {
        val id = NodeIdImpl(uuid4().toString())
        val ok = driver.transactionWithRollback {
            createElement(fieldMap, id, name, ElementType.Node)
            true
        }.getOrDefault(false)
        return if (ok) {
            Node(id, name, fieldMap)
        } else {
            null
        }
    }

    override fun <S : Schema> createOrReplaceNode(name: String, fieldMap: FieldMap<S>): Node<S> {
        val id = NodeIdImpl(uuid4().toString())
        return driver.transaction {
            val oldElementId = getElementId(driver, name)
            if (oldElementId != null) {
                val oldSchema = findElementSchema(oldElementId)
                    ?: error("Could not find schema for node name=$name")
                deleteFieldValues(oldElementId, oldSchema)
                createElement(fieldMap, id, name, ElementType.Node, updateOrReplace = true)
            } else {
                createElement(fieldMap, id, name, ElementType.Node)
            }
            Node(id, name, fieldMap)
        }
    }

    override fun deleteNode(name: String, withConnections: Boolean): Boolean {
        return deleteElementByName(name, withConnections)
    }

    override fun deleteNode(id: NodeId, withConnections: Boolean): Boolean {
        return deleteElementById(id, withConnections)
    }

    override fun findNodeSchema(id: NodeId): Schema? {
        return findElementSchema(id)
    }

    override fun <S : Schema> getOrCreateNode(name: String, fieldMap: FieldMap<S>): Node<S> {
        return driver.transaction {
            query(NodeMatch(fieldMap.schema(), Where.name(name))).firstOrNull() ?: run {
                val id = NodeIdImpl(uuid4().toString())
                createElement(fieldMap, id, name, ElementType.Node)
                Node(id, name, fieldMap)
            }
        }
    }

    override fun <S : Schema, T> updateField(node: Node<S>, field: Field<S, T>, value: T): Node<S> {
        return driver.transaction {
            updateFieldValue(node.id, node.fieldMap.schema(), field, value)
            query(NodeMatch(node.fieldMap.schema(), Where.id(node.id))).first()
        }
    }

    override fun <S : Schema, T> updateField(
        node: Node<S>,
        field: Field<S, T>,
        value: (T) -> T
    ): Node<S> {
        return updateField(node, field, value(node[field]))
    }

    override fun <S : Schema> updateFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S> {
        return driver.transaction {
            val updatedId = updateFieldValues(
                node.name,
                node.id,
                node.fieldMap.schema(),
                fieldMap
            ) { NodeIdImpl(uuid4().toString()) }
            Node(updatedId, node.name, fieldMap)
        }
    }

    override fun <S : Schema> query(match: EdgeMatch<S>): Sequence<Edge<S>> {
        return driver.transaction {
            val (schema, idToNameSeq) = performQuery(match)
            idToNameSeq.map { (id, name) ->
                val fieldMap = getFieldMap(id, schema)
                Edge(EdgeIdImpl(id), name, fieldMap)
            }
        }
    }

    override fun <S : Schema> query(match: NodeMatch<S>): Sequence<Node<S>> {
        return driver.transaction {
            val (schema, idToNameSeq) = performQuery(match)
            idToNameSeq.map { (id, name) ->
                val fieldMap = getFieldMap(id, schema)
                Node(NodeIdImpl(id), name, fieldMap)
            }
        }
    }

    override fun <T> transaction(fn: GraphLiteDatabase.() -> T): T {
        return driver.transaction {
            fn(this)
        }
    }

    // Helpers

    private fun <S : Schema> createElement(
        fieldMap: FieldMap<S>,
        elementId: ElementId,
        name: String,
        type: ElementType,
        updateOrReplace: Boolean = false
    ) {
        require(name.isNotEmpty())

        val schema = fieldMap.schema()
        val schemaId = getSchemaId(driver, schema)
        val elementValues = SqlContentValues().apply {
            put("id", elementId.toString())
            put("name", name)
            put("schemaId", schemaId)
            put("type", type.code)
        }

        if (updateOrReplace) {
            driver.updateOrReplace("Element", elementValues, "name = ?", arrayOf(name))
        } else {
            driver.insertOrAbortAndThrow("Element", elementValues)
        }

        for ((_, field) in schema.getFields<S>()) {
            val fieldValueId = uuid4().toString()
            val fieldId = getFieldId(driver, field, schemaId)
            val value = fieldMap[field]
            val tableName = getFieldValueTableName(fieldId)

            val contentValues = SqlContentValues().apply {
                put("id", fieldValueId)
                put("elementId", elementId.toString())
            }
            fillFieldValue(field.type, value, contentValues)
            driver.insertOrAbortAndThrow(tableName, contentValues)
        }
    }

    private fun deleteElementByName(name: String, withConnections: Boolean): Boolean {
        if (withConnections) deleteElementConnections(name)
        return driver.delete("Element", "name = ?", arrayOf(name))
    }

    private fun deleteElementById(id: ElementId, withConnections: Boolean): Boolean {
        if (withConnections) {
            getElementName(driver, id)?.let {
                deleteElementConnections(it)
            }
        }
        return driver.delete("Element", "id = ?", arrayOf(id.toString()))
    }

    private fun deleteElementConnections(elementName: String) {
        driver.delete(
            "Connection",
            "edgeName = ? OR nodeName = ?",
            arrayOf(elementName, elementName)
        )
    }

    private fun fillFieldValue(fieldType: String, value: Any?, into: SqlContentValues) {
        val baseFieldType = baseFieldTypeName(fieldType)
        if (baseFieldType == Field.FIELD_TYPE_BLOB && value is ByteArray?) {
            into.put("value", value)
        } else if (baseFieldType == Field.FIELD_TYPE_GEO && value is GeoBounds?) {
            into.put("minLat", value?.minLat)
            into.put("maxLat", value?.maxLat)
            into.put("minLon", value?.minLon)
            into.put("maxLon", value?.maxLon)
        } else if (baseFieldType == Field.FIELD_TYPE_LONG_INT && value is Long?) {
            into.put("value", value)
        } else if (baseFieldType == Field.FIELD_TYPE_DOUBLE_FLOAT && value is Double?) {
            into.put("value", value)
        } else if (baseFieldType == Field.FIELD_TYPE_TEXT && value is String?) {
            into.put("value", value)
        }
    }

    private fun findElementSchema(elementId: ElementId): Schema? {
        return driver.transaction {
            val (schemaId, schema) = driver.query(
                "SELECT id, name, version FROM Schema WHERE id = (SELECT schemaId FROM Element WHERE id = ?)",
                arrayOf(elementId.toString())
            ).use {
                if (it.moveToNext()) {
                    val id = it.getString("id")
                    val name = it.getString("name")
                    val version = it.getLong("version")
                    Pair(id, object : Schema(name, version) {})
                } else {
                    return@transaction null
                }
            }
            getSchemaFields(driver, schemaId).forEach {
                schema.addField(it)
            }
            schema.freeze()
            schema
        }
    }

    private fun <S : Schema> getFieldMap(
        elementId: String,
        schema: S
    ): FieldMap<S> {
        val fields: Collection<Field<S, Any?>> = schema.getFields<S>().values
        val fieldMap = MutableFieldMapImpl(schema)
        for (field in fields) {
            val value: Any? = getFieldValue(elementId, schema, field)
            fieldMap[field] = value
        }
        return fieldMap
    }

    private fun <S : Schema, T> getFieldValue(
        elementId: String,
        schema: S,
        field: Field<S, T>
    ): T {
        val fieldId = getFieldId(driver, field, schema)
        val fieldValueTableName = getFieldValueTableName(fieldId)
        val value: Any? = driver.query(
            "SELECT * FROM $fieldValueTableName WHERE elementId = ?",
            arrayOf(elementId)
        ).use { cursor ->
            when {
                cursor.moveToNext() -> readPlainFieldValue<Any?>(cursor, field.type)
                else -> error("Missing field value: elementId=$elementId field=$field schema=$schema")
            }
        }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    private fun getSchemaId(driver: SqliteDriver, schema: Schema): String {
        return driver.query(
            "SELECT id FROM Schema WHERE name = ? AND version = ?",
            arrayOf(schema.schemaName, schema.schemaVersion.toString())
        ).use {
            if (it.moveToNext()) {
                it.getString("id")
            } else {
                error("Schema $schema was not found.")
            }
        }
    }

    private fun <S : Schema> performQuery(match: ElementMatch<S>): Pair<S, Sequence<Pair<String, String>>> {
        return when (match) {
            is ElementMatchImpl<S> -> queryEngine.performQuery(match)
            else -> error("Unknown ElementMatch subclass: $match")
        }
    }

    private fun <T> readPlainFieldValue(cursor: SqliteCursorFacade, fieldType: String): T {
        val isNullable = isFieldTypeNullable(fieldType)
        val value: Any? = when (baseFieldTypeName(fieldType)) {
            Field.FIELD_TYPE_BLOB -> {
                if (isNullable) {
                    cursor.findBlob("value")
                } else {
                    cursor.getBlob("value")
                }
            }
            Field.FIELD_TYPE_GEO -> {
                if (isNullable) {
                    val minLat = cursor.findDouble("minLat")
                    val maxLat = cursor.findDouble("maxLat")
                    val minLon = cursor.findDouble("minLon")
                    val maxLon = cursor.findDouble("maxLon")
                    if (minLat != null && maxLat != null && minLon != null && maxLon != null) {
                        GeoBounds(minLat, maxLat, minLon, maxLon)
                    } else {
                        null
                    }
                } else {
                    val minLat = cursor.getDouble("minLat")
                    val maxLat = cursor.getDouble("maxLat")
                    val minLon = cursor.getDouble("minLon")
                    val maxLon = cursor.getDouble("maxLon")
                    GeoBounds(minLat, maxLat, minLon, maxLon)
                }
            }
            Field.FIELD_TYPE_LONG_INT -> {
                if (isNullable) {
                    cursor.findLong("value")
                } else {
                    cursor.getLong("value")
                }
            }
            Field.FIELD_TYPE_DOUBLE_FLOAT -> {
                if (isNullable) {
                    cursor.findDouble("value")
                } else {
                    cursor.getDouble("value")
                }
            }
            Field.FIELD_TYPE_TEXT -> {
                if (isNullable) {
                    cursor.findString("value")
                } else {
                    cursor.getString("value")
                }
            }
            else -> error("Unknown field type: $fieldType")
        }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    private fun <S : Schema, T> updateFieldValue(
        elementId: ElementId,
        schema: S,
        field: Field<S, T>,
        value: T
    ) {
        val fieldId = getFieldId(driver, field, schema)
        updateFieldValue(elementId, fieldId, field.type, value)
    }

    private fun <S : Schema, T> updateFieldValue(
        elementId: ElementId,
        schemaId: String,
        field: Field<S, T>,
        value: T
    ) {
        val fieldId = getFieldId(driver, field, schemaId)
        updateFieldValue(elementId, fieldId, field.type, value)
    }

    private fun <T> updateFieldValue(
        elementId: ElementId,
        fieldId: FieldId,
        fieldType: String,
        value: T
    ) {
        val fieldValueTableName = getFieldValueTableName(fieldId)

        driver.delete(fieldValueTableName, "elementId = ?", arrayOf(elementId.toString()))

        val fieldValueId = uuid4().toString()
        val contentValues = SqlContentValues().apply {
            put("id", fieldValueId)
            put("elementId", elementId.toString())
        }
        fillFieldValue(fieldType, value, contentValues)
        driver.insertOrAbortAndThrow(fieldValueTableName, contentValues)
    }

    private fun <S : Schema, ID : ElementId> updateFieldValues(
        elementName: String,
        oldElementId: ID,
        oldSchema: Schema,
        newFieldMap: FieldMap<S>,
        idFactory: () -> ID
    ): ID {
        val newSchema = newFieldMap.schema()
        val newSchemaId = getSchemaId(driver, newSchema)
        val hasNewSchema = oldSchema != newSchema
        val elementId = if (hasNewSchema) idFactory() else oldElementId

        if (hasNewSchema) {
            deleteFieldValues(oldElementId, oldSchema)
            val elementValues = SqlContentValues().apply {
                put("id", elementId.toString())
                put("schemaId", newSchemaId)
            }
            driver.updateOrReplace("Element", elementValues, "name = ?", arrayOf(elementName))
        }

        for ((_, field) in newSchema.getFields<S>()) {
            val value = newFieldMap[field]
            updateFieldValue(elementId, newSchemaId, field, value)
        }

        return elementId
    }

    private fun deleteFieldValues(elementId: ElementId, schema: Schema) {
        for ((_, field) in schema.getFields<Schema>()) {
            val fieldId = getFieldId(driver, field, schema)
            val tableName = getFieldValueTableName(fieldId)
            driver.delete(tableName, "elementId = ?", arrayOf(elementId.toString()))
        }
    }
}
