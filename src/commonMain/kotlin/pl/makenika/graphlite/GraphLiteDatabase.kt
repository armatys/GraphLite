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

package pl.makenika.graphlite

import kotlinx.coroutines.flow.Flow

interface GraphLiteDatabase {
    fun close()

    suspend fun connect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    suspend fun connect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    suspend fun connectOrReplace(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    suspend fun connectOrReplace(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    suspend fun disconnect(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Boolean
    fun getConnections(elementHandle: ElementHandle): Flow<Connection>
    suspend fun getOrConnect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    suspend fun getOrConnect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    suspend fun findConnection(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Connection?

    suspend fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema> createEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>?
    suspend fun <S : Schema> createEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>?
    suspend fun <S : Schema> createOrReplaceEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema> createOrReplaceEdge(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S>

    suspend fun deleteEdge(handle: EdgeHandle, withConnections: Boolean = true): Boolean
    suspend fun deleteEdge(handleValue: String, withConnections: Boolean = true): Boolean
    suspend fun findEdgeSchema(handle: EdgeHandle): Schema?
    suspend fun findEdgeSchema(handleValue: String): Schema?
    suspend fun <S : Schema> getOrCreateEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema> getOrCreateEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema, T> updateEdgeField(
        edge: Edge<S>,
        field: Field<S, T>,
        value: T
    ): Edge<S>

    suspend fun <S : Schema> updateEdgeFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema> updateEdgeFields(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    suspend fun <S : Schema> updateEdgeFields(handleValue: String, fieldMap: FieldMap<S>): Edge<S>

    suspend fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema> createNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>?
    suspend fun <S : Schema> createNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>?
    suspend fun <S : Schema> createOrReplaceNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema> createOrReplaceNode(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S>

    suspend fun deleteNode(handle: NodeHandle, withConnections: Boolean = true): Boolean
    suspend fun deleteNode(handleValue: String, withConnections: Boolean = true): Boolean
    suspend fun findNodeSchema(handle: NodeHandle): Schema?
    suspend fun findNodeSchema(handleValue: String): Schema?
    suspend fun <S : Schema> getOrCreateNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema> getOrCreateNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema, T> updateEdgeField(
        node: Node<S>,
        field: Field<S, T>,
        value: T
    ): Node<S>

    suspend fun <S : Schema> updateNodeFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema> updateNodeFields(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    suspend fun <S : Schema> updateNodeFields(handleValue: String, fieldMap: FieldMap<S>): Node<S>

    fun <S : Schema> query(match: EdgeMatch<S>): Flow<Edge<S>>
    fun <S : Schema> query(match: NodeMatch<S>): Flow<Node<S>>

    suspend fun <T> transaction(fn: suspend GraphLiteDatabase.() -> T): T
}
