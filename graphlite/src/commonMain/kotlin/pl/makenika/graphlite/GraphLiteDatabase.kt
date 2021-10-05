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

public interface GraphLiteDatabase {
    public fun close()

    public fun connect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    public fun connect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    public fun connectOrReplace(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    public fun connectOrReplace(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    public fun disconnect(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Boolean
    public fun getConnections(elementHandle: ElementHandle): Sequence<Connection>
    public fun getOrConnect(
        edgeHandle: EdgeHandle,
        nodeHandle: NodeHandle,
        outgoing: Boolean? = null
    ): Connection

    public fun getOrConnect(
        edgeHandle: EdgeHandle,
        sourceNodeHandle: NodeHandle,
        targetNodeHandle: NodeHandle,
        directed: Boolean
    ): Collection<Connection>

    public fun findConnection(edgeHandle: EdgeHandle, nodeHandle: NodeHandle): Connection?

    public fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S>
    public fun <S : Schema> createEdge(handle: EdgeHandle, fieldMap: FieldMap<S>): Edge<S>?
    public fun <S : Schema> createEdge(handleValue: String, fieldMap: FieldMap<S>): Edge<S>?
    public fun <S : Schema> createOrReplaceEdge(
        handle: EdgeHandle,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun <S : Schema> createOrReplaceEdge(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun deleteEdge(handle: EdgeHandle, withConnections: Boolean = true): Boolean
    public fun deleteEdge(handleValue: String, withConnections: Boolean = true): Boolean
    public fun findEdgeSchema(handle: EdgeHandle): Schema?
    public fun findEdgeSchema(handleValue: String): Schema?
    public fun <S : Schema> getOrCreateEdge(
        handle: EdgeHandle,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun <S : Schema> getOrCreateEdge(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun <S : Schema, T> updateEdgeField(
        edge: Edge<S>,
        field: Field<S, T>,
        value: T
    ): Edge<S>

    public fun <S : Schema> updateEdgeFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S>
    public fun <S : Schema> updateEdgeFields(
        handle: EdgeHandle,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun <S : Schema> updateEdgeFields(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Edge<S>

    public fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S>
    public fun <S : Schema> createNode(handle: NodeHandle, fieldMap: FieldMap<S>): Node<S>?
    public fun <S : Schema> createNode(handleValue: String, fieldMap: FieldMap<S>): Node<S>?
    public fun <S : Schema> createOrReplaceNode(
        handle: NodeHandle,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun <S : Schema> createOrReplaceNode(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun deleteNode(handle: NodeHandle, withConnections: Boolean = true): Boolean
    public fun deleteNode(handleValue: String, withConnections: Boolean = true): Boolean
    public fun findNodeSchema(handle: NodeHandle): Schema?
    public fun findNodeSchema(handleValue: String): Schema?
    public fun <S : Schema> getOrCreateNode(
        handle: NodeHandle,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun <S : Schema> getOrCreateNode(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun <S : Schema, T> updateNodeField(
        node: Node<S>,
        field: Field<S, T>,
        value: T
    ): Node<S>

    public fun <S : Schema> updateNodeFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S>
    public fun <S : Schema> updateNodeFields(
        handle: NodeHandle,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun <S : Schema> updateNodeFields(
        handleValue: String,
        fieldMap: FieldMap<S>
    ): Node<S>

    public fun <S : Schema> query(match: EdgeMatch<S>): Sequence<Edge<S>>
    public fun <S : Schema> query(match: NodeMatch<S>): Sequence<Node<S>>

    public fun <T> transaction(fn: GraphLiteDatabase.() -> T): T
}
