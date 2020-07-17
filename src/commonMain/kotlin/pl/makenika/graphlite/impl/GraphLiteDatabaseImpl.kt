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

import com.benasher44.uuid.Uuid
import com.benasher44.uuid.uuid4
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import pl.makenika.graphlite.*
import pl.makenika.graphlite.impl.GraphSqlUtils.getElementId
import pl.makenika.graphlite.impl.GraphSqlUtils.getFieldId
import pl.makenika.graphlite.impl.GraphSqlUtils.getSchemaFields
import pl.makenika.graphlite.sql.*

internal class GraphLiteDatabaseImpl internal constructor(private val driver: SqliteDriver) :
    GraphLiteDatabase {
    private val queryEngine by lazy { QueryEngine(driver) }

    override fun close() {
        driver.close()
    }

    override fun connect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean?
    ): Connection {
        val connId = uuid4()
        val values = SqlContentValues().apply {
            put("id", connId.toString())
            put("edgeHandle", edgeHandle.value)
            put("nodeHandle", nodeHandle.value)
            put("outgoing", outgoing?.let { if (it) 1 else 0 })
        }
        driver.insertOrAbortAndThrow("Connection", values)
        return Connection(edgeHandle, nodeHandle, outgoing)
    }

    override fun connect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection> {
        require(sourceNodeHandle != targetNodeHandle) { "Cannot connect the same node handle=$sourceNodeHandle." }
        return driver.transaction {
            val a = connect(edgeHandle, sourceNodeHandle, if (directed) true else null)
            val b = connect(edgeHandle, targetNodeHandle, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun connectOrReplace(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean?
    ): Connection {
        return transaction {
            disconnect(edgeHandle, nodeHandle)
            connect(edgeHandle, nodeHandle, outgoing)
        }
    }

    override fun connectOrReplace(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection> {
        return transaction {
            val a = connectOrReplace(edgeHandle, sourceNodeHandle, if (directed) true else null)
            val b = connectOrReplace(edgeHandle, targetNodeHandle, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun disconnect(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Boolean {
        return driver.delete(
            "Connection",
            "edgeHandle = ? AND nodeHandle = ?",
            arrayOf(edgeHandle.value, nodeHandle.value)
        )
    }

    override fun getConnections(elementHandle: ElementHandle): Flow<Connection> {
        return flow {
            driver.query(
                "SELECT * FROM Connection WHERE edgeHandle = ? OR nodeHandle = ?", arrayOf(
                    elementHandle.value,
                    elementHandle.value
                )
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    val edgeHandle = cursor.getString("edgeHandle")
                    val nodeHandle = cursor.getString("nodeHandle")
                    val outgoing = cursor.findBoolean("outgoing")
                    emit(Connection(EdgeHandle(edgeHandle), NodeHandle(nodeHandle), outgoing))
                }
            }
        }
    }

    override fun getOrConnect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean?
    ): Connection {
        return transaction {
            findConnection(edgeHandle, nodeHandle) ?: connect(edgeHandle, nodeHandle, outgoing)
        }
    }

    override fun getOrConnect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection> {
        return transaction {
            val a = getOrConnect(edgeHandle, sourceNodeHandle, if (directed) true else null)
            val b = getOrConnect(edgeHandle, targetNodeHandle, if (directed) false else null)
            listOf(a, b)
        }
    }

    override fun findConnection(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Connection? {
        val bindings = arrayOf(edgeHandle.value, nodeHandle.value)
        return driver.transaction {
            driver.query(
                "SELECT * FROM Connection WHERE edgeHandle = ? AND nodeHandle = ?",
                bindings
            )
                .use { cursor ->
                    if (cursor.moveToNext()) {
                        val eHandle = cursor.getString("edgeHandle")
                        val nHandle = cursor.getString("nodeHandle")
                        val outgoing = cursor.findBoolean("outgoing")
                        Connection(EdgeHandle(eHandle), NodeHandle(nHandle), outgoing)
                    } else {
                        null
                    }
                }
        }
    }

    override fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S> {
        val id = uuid4()
        val handle = EdgeHandle(uuid4().toString())
        driver.transaction {
            createElement(fieldMap, id, handle, ElementType.Edge)
        }
        return Edge(handle, fieldMap)
    }

    override fun <S : Schema> createEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>? {
        val id = uuid4()
        val ok = driver.transactionWithRollback {
            createElement(fieldMap, id, handle, ElementType.Edge)
            true
        }.getOrDefault(false)
        return if (ok) {
            Edge(handle, fieldMap)
        } else {
            null
        }
    }

    override fun <S : Schema> createEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>? {
        return createEdge(EdgeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema> createOrReplaceEdge(
        handle: EdgeHandle,
        fieldMap: FieldMap<S>
    ): Edge<S> {
        val id = uuid4()
        return driver.transaction {
            val oldElementId = getElementId(driver, handle)
            if (oldElementId != null) {
                val oldSchema =
                    findElementSchema(handle)
                        ?: error("Could not find schema for edge handle=$handle")
                deleteFieldValues(oldElementId, oldSchema)
                createElement(fieldMap, id, handle, ElementType.Edge, updateOrReplace = true)
            } else {
                createElement(fieldMap, id, handle, ElementType.Edge)
            }
            Edge(handle, fieldMap)
        }
    }

    override fun <S : Schema> createOrReplaceEdge(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S> {
        return createOrReplaceEdge(EdgeHandle(handleValue), fieldMap)
    }

    override fun deleteEdge(handle: EdgeHandle, withConnections: Boolean): Boolean {
        return deleteElementByHandle(handle, withConnections)
    }

    override fun deleteEdge(handleValue: String, withConnections: Boolean): Boolean {
        return deleteEdge(EdgeHandle(handleValue), withConnections)
    }

    override fun findEdgeSchema(handle: EdgeHandle): Schema? {
        return findElementSchema(handle)
    }

    override fun findEdgeSchema(handleValue: String): Schema? {
        return findEdgeSchema(EdgeHandle(handleValue))
    }

    override fun <S : Schema> getOrCreateEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S> {
        return driver.transaction {
            findElement(handle, fieldMap.schema(), ::Edge) ?: run {
                val id = uuid4()
                createElement(fieldMap, id, handle, ElementType.Edge)
                Edge(handle, fieldMap)
            }
        }
    }

    override fun <S : Schema> getOrCreateEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S> {
        return getOrCreateEdge(EdgeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema, T> updateEdgeField(
        edge: Edge<S>,
        field: Field<S, T>,
        value: T
    ): Edge<S> {
        return driver.transaction {
            val elementId = getElementId(driver, edge.handle)!!
            updateFieldValue(elementId, edge.fieldMap.schema(), field, value)
            findElement(edge.handle, edge.fieldMap.schema(), ::Edge)!!
        }
    }

    override fun <S : Schema> updateEdgeFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S> {
        return driver.transaction {
            updateFieldValues(
                edge.handle,
                edge.fieldMap.schema(),
                fieldMap
            )
            Edge(edge.handle, fieldMap)
        }
    }

    override fun <S : Schema> updateEdgeFields(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S> {
        return driver.transaction {
            val schema = findElementSchema(handle) ?: error("Could not find schema for $handle")
            updateFieldValues(
                handle,
                schema,
                fieldMap
            )
            Edge(handle, fieldMap)
        }
    }

    override fun <S : Schema> updateEdgeFields(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S> {
        return updateEdgeFields(EdgeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S> {
        val id = uuid4()
        val handle = NodeHandle(uuid4().toString())
        driver.transaction {
            createElement(fieldMap, id, handle, ElementType.Node)
        }
        return Node(handle, fieldMap)
    }

    override fun <S : Schema> createNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>? {
        val id = uuid4()
        val ok = driver.transactionWithRollback {
            createElement(fieldMap, id, handle, ElementType.Node)
            true
        }.getOrDefault(false)
        return if (ok) {
            Node(handle, fieldMap)
        } else {
            null
        }
    }

    override fun <S : Schema> createNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>? {
        return createNode(NodeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema> createOrReplaceNode(
        handle: NodeHandle,
        fieldMap: FieldMap<S>
    ): Node<S> {
        val id = uuid4()
        return driver.transaction {
            val oldElementId = getElementId(driver, handle)
            if (oldElementId != null) {
                val oldSchema =
                    findElementSchema(handle)
                        ?: error("Could not find schema for node handle=$handle")
                deleteFieldValues(oldElementId, oldSchema)
                createElement(fieldMap, id, handle, ElementType.Node, updateOrReplace = true)
            } else {
                createElement(fieldMap, id, handle, ElementType.Node)
            }
            Node(handle, fieldMap)
        }
    }

    override fun <S : Schema> createOrReplaceNode(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S> {
        return createOrReplaceNode(NodeHandle(handleValue), fieldMap)
    }

    override fun deleteNode(handle: NodeHandle, withConnections: Boolean): Boolean {
        return deleteElementByHandle(handle, withConnections)
    }

    override fun deleteNode(handleValue: String, withConnections: Boolean): Boolean {
        return deleteNode(NodeHandle(handleValue), withConnections)
    }

    override fun findNodeSchema(handle: NodeHandle): Schema? {
        return findElementSchema(handle)
    }

    override fun findNodeSchema(handleValue: String): Schema? {
        return findNodeSchema(NodeHandle(handleValue))
    }

    override fun <S : Schema> getOrCreateNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S> {
        return driver.transaction {
            findElement(handle, fieldMap.schema(), ::Node) ?: run {
                val id = uuid4()
                createElement(fieldMap, id, handle, ElementType.Node)
                Node(handle, fieldMap)
            }
        }
    }

    override fun <S : Schema> getOrCreateNode(handleValue: String, fieldMap: FieldMap<S>): Node<S> {
        return getOrCreateNode(NodeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema, T> updateEdgeField(
        node: Node<S>,
        field: Field<S, T>,
        value: T
    ): Node<S> {
        return driver.transaction {
            val elementId =
                getElementId(driver, node.handle) ?: error("Node ${node.handle} not found.")
            updateFieldValue(elementId, node.fieldMap.schema(), field, value)
            findElement(node.handle, node.fieldMap.schema(), ::Node)!!
        }
    }

    override fun <S : Schema> updateNodeFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S> {
        return driver.transaction {
            updateFieldValues(
                node.handle,
                node.fieldMap.schema(),
                fieldMap
            )
            Node(node.handle, fieldMap)
        }
    }

    override fun <S : Schema> updateNodeFields(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S> {
        return driver.transaction {
            val schema = findElementSchema(handle) ?: error("Could not find schema for $handle")
            updateFieldValues(
                handle,
                schema,
                fieldMap
            )
            Node(handle, fieldMap)
        }
    }

    override fun <S : Schema> updateNodeFields(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S> {
        return updateNodeFields(NodeHandle(handleValue), fieldMap)
    }

    override fun <S : Schema> query(match: EdgeMatch<S>): Flow<Edge<S>> {
        return driver.transaction {
            val (schema, idToHandleValueFlow) = performQuery(match)
            idToHandleValueFlow.map { (id, handleValue) ->
                val fieldMap = getFieldMap(id, schema)
                Edge(EdgeHandle(handleValue), fieldMap)
            }
        }
    }

    override fun <S : Schema> query(match: NodeMatch<S>): Flow<Node<S>> {
        return driver.transaction {
            val (schema, idToHandleValueSeq) = performQuery(match)
            idToHandleValueSeq.map { (id, handleValue) ->
                val fieldMap = getFieldMap(id, schema)
                Node(NodeHandle(handleValue), fieldMap)
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
        elementId: Uuid,
        handle: ElementHandle,
        type: ElementType,
        updateOrReplace: Boolean = false
    ) {
        require(handle.value.isNotEmpty())

        val schema = fieldMap.schema()
        val schemaId = getSchemaId(driver, schema)
        val elementValues = SqlContentValues().apply {
            put("id", elementId.toString())
            put("handle", handle.value)
            put("schemaId", schemaId)
            put("type", type.code)
        }

        if (updateOrReplace) {
            driver.updateOrReplace("Element", elementValues, "handle = ?", arrayOf(handle.value))
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

    private fun deleteElementByHandle(handle: ElementHandle, withConnections: Boolean): Boolean {
        if (withConnections) deleteElementConnections(handle)
        return driver.delete("Element", "handle = ?", arrayOf(handle.value))
    }

    private fun deleteElementConnections(handle: ElementHandle) {
        driver.delete(
            "Connection",
            "edgeHandle = ? OR nodeHandle = ?",
            arrayOf(handle.value, handle.value)
        )
    }

    private fun fillFieldValue(fieldType: FieldType, value: Any?, into: SqlContentValues) {
        when (fieldType) {
            is FieldType.Blob -> into.put("value", value as ByteArray?)
            is FieldType.Geo -> {
                val bounds = value as GeoBounds?
                into.put("minLat", bounds?.minLat)
                into.put("maxLat", bounds?.maxLat)
                into.put("minLon", bounds?.minLon)
                into.put("maxLon", bounds?.maxLon)
            }
            is FieldType.LongInt -> into.put("value", value as Long?)
            is FieldType.DoubleFloat -> into.put("value", value as Double?)
            is FieldType.Text -> into.put("value", value as String?)
            is FieldType.TextFts -> into.put("value", value as String?)
        }.hashCode()
    }

    private fun <T : GraphElement<S>, S : Schema, H : ElementHandle> findElement(
        handle: H,
        schema: S,
        elementFactory: (H, FieldMap<S>) -> T
    ): T? {
        driver.query("SELECT id FROM Element WHERE handle = ?", arrayOf(handle.value)).use {
            if (!it.moveToNext()) {
                return null
            }
            val id = it.getString("id")
            val fieldMap = getFieldMap(id, schema)
            return elementFactory(handle, fieldMap)
        }
    }

    private fun findElementSchema(handle: ElementHandle): Schema? {
        return driver.transaction {
            val (schemaId, schema) = driver.query(
                "SELECT id, handle, version FROM Schema WHERE id = (SELECT schemaId FROM Element WHERE handle = ?)",
                arrayOf(handle.value)
            ).use {
                if (it.moveToNext()) {
                    val id = it.getString("id")
                    val handleValue = it.getString("handle")
                    val version = it.getLong("version")
                    Pair(id, object : Schema(SchemaHandle(handleValue), version) {})
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
            "SELECT id FROM Schema WHERE handle = ? AND version = ?",
            arrayOf(schema.schemaHandle.value, schema.schemaVersion.toString())
        ).use {
            if (it.moveToNext()) {
                it.getString("id")
            } else {
                error("Schema $schema was not found.")
            }
        }
    }

    private fun <S : Schema> performQuery(match: ElementMatch<S>): Pair<S, Flow<Pair<String, String>>> {
        val m = match as? ElementMatchImpl<S> ?: error("Unknown ElementMatch subclass: $match")
        return queryEngine.performQuery(m)
    }

    private fun <T> readPlainFieldValue(cursor: SqliteCursorFacade, fieldType: FieldType): T {
        val isNullable = fieldType.optional
        val value: Any? = when (fieldType) {
            is FieldType.Blob -> {
                if (isNullable) {
                    cursor.findBlob("value")
                } else {
                    cursor.getBlob("value")
                }
            }
            is FieldType.Geo -> {
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
            is FieldType.LongInt -> {
                if (isNullable) {
                    cursor.findLong("value")
                } else {
                    cursor.getLong("value")
                }
            }
            is FieldType.DoubleFloat -> {
                if (isNullable) {
                    cursor.findDouble("value")
                } else {
                    cursor.getDouble("value")
                }
            }
            is FieldType.Text -> {
                if (isNullable) {
                    cursor.findString("value")
                } else {
                    cursor.getString("value")
                }
            }
            is FieldType.TextFts -> {
                if (isNullable) {
                    cursor.findString("value")
                } else {
                    cursor.getString("value")
                }
            }
        }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    private fun <S : Schema, T> updateFieldValue(
        elementId: String,
        schema: S,
        field: Field<S, T>,
        value: T
    ) {
        val fieldId = getFieldId(driver, field, schema)
        updateFieldValue(elementId, fieldId, field.type, value)
    }

    private fun <S : Schema, T> updateFieldValue(
        elementId: String,
        schemaId: String,
        field: Field<S, T>,
        value: T
    ) {
        val fieldId = getFieldId(driver, field, schemaId)
        updateFieldValue(elementId, fieldId, field.type, value)
    }

    private fun <T> updateFieldValue(
        elementId: String,
        fieldId: String,
        fieldType: FieldType,
        value: T
    ) {
        val fieldValueTableName = getFieldValueTableName(fieldId)

        driver.delete(fieldValueTableName, "elementId = ?", arrayOf(elementId))

        val fieldValueId = uuid4().toString()
        val contentValues = SqlContentValues().apply {
            put("id", fieldValueId)
            put("elementId", elementId)
        }
        fillFieldValue(fieldType, value, contentValues)
        driver.insertOrAbortAndThrow(fieldValueTableName, contentValues)
    }

    private fun <S : Schema> updateFieldValues(
        handle: ElementHandle,
        oldSchema: Schema,
        newFieldMap: FieldMap<S>
    ): String {
        val newSchema = newFieldMap.schema()
        val newSchemaId = getSchemaId(driver, newSchema)
        val hasNewSchema = oldSchema != newSchema
        val oldElementId = getElementId(driver, handle)!!
        val elementId = if (hasNewSchema) uuid4().toString() else oldElementId

        if (hasNewSchema) {
            deleteFieldValues(oldElementId, oldSchema)
            val elementValues = SqlContentValues().apply {
                put("id", elementId)
                put("schemaId", newSchemaId)
            }
            driver.updateOrReplace("Element", elementValues, "handle = ?", arrayOf(handle.value))
        }

        for ((_, field) in newSchema.getFields<S>()) {
            val value = newFieldMap[field]
            updateFieldValue(elementId, newSchemaId, field, value)
        }

        return elementId
    }

    private fun deleteFieldValues(elementId: String, schema: Schema) {
        for ((_, field) in schema.getFields<Schema>()) {
            val fieldId = getFieldId(driver, field, schema)
            val tableName = getFieldValueTableName(fieldId)
            driver.delete(tableName, "elementId = ?", arrayOf(elementId))
        }
    }
}
