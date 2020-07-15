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

interface GraphLiteDatabase {
    fun close()

    fun connect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    fun connect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    fun connectOrReplace(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    fun connectOrReplace(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    fun disconnect(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Boolean
    fun getConnections(elementHandle: ElementHandle): Sequence<Connection>
    fun getOrConnect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    fun getOrConnect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    fun findConnection(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Connection?

    fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> createEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>?
    fun <S : Schema> createEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>?
    fun <S : Schema> createOrReplaceEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> createOrReplaceEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>
    fun deleteEdge(handle: EdgeHandle, withConnections: Boolean = true): Boolean
    fun deleteEdge(handleValue: String, withConnections: Boolean = true): Boolean
    fun findEdgeSchema(handle: EdgeHandle): Schema?
    fun findEdgeSchema(handleValue: String): Schema?
    fun <S : Schema> getOrCreateEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> getOrCreateEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema, T> updateEdgeField(edge: Edge<S>, field: Field<S, T>, value: T): Edge<S>
    fun <S : Schema> updateEdgeFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> updateEdgeFields(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> updateEdgeFields(handleValue: String, fieldMap: FieldMap<S>): Edge<S>

    fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> createNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>?
    fun <S : Schema> createNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>?
    fun <S : Schema> createOrReplaceNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> createOrReplaceNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>
    fun deleteNode(handle: NodeHandle, withConnections: Boolean = true): Boolean
    fun deleteNode(handleValue: String, withConnections: Boolean = true): Boolean
    fun findNodeSchema(handle: NodeHandle): Schema?
    fun findNodeSchema(handleValue: String): Schema?
    fun <S : Schema> getOrCreateNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> getOrCreateNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema, T> updateEdgeField(node: Node<S>, field: Field<S, T>, value: T): Node<S>
    fun <S : Schema> updateNodeFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> updateNodeFields(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> updateNodeFields(handleValue: String, fieldMap: FieldMap<S>): Node<S>

    fun <S : Schema> query(match: EdgeMatch<S>): Sequence<Edge<S>>
    fun <S : Schema> query(match: NodeMatch<S>): Sequence<Node<S>>

    fun <T> transaction(fn: GraphLiteDatabase.() -> T): T
}
