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

    fun connect(edgeName: String, nodeName: String, outgoing: Boolean? = null): Connection
    fun connect(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection>

    fun connectOrReplace(edgeName: String, nodeName: String, outgoing: Boolean? = null): Connection
    fun connectOrReplace(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection>

    fun disconnect(edgeName: String, nodeName: String): Boolean
    fun getConnections(elementName: String): Sequence<Connection>
    fun getOrConnect(edgeName: String, nodeName: String, outgoing: Boolean? = null): Connection
    fun getOrConnect(
        edgeName: String,
        sourceNodeName: String,
        targetNodeName: String,
        directed: Boolean
    ): Collection<Connection>

    fun findConnection(edgeName: String, nodeName: String): Connection?

    fun <S : Schema> createEdge(fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema> createEdge(name: String, fieldMap: FieldMap<S>): Edge<S>?
    fun <S : Schema> createOrReplaceEdge(name: String, fieldMap: FieldMap<S>): Edge<S>
    fun deleteEdge(name: String, withConnections: Boolean = true): Boolean
    fun deleteEdge(id: EdgeId, withConnections: Boolean = true): Boolean
    fun findEdgeSchema(id: EdgeId): Schema?
    fun <S : Schema> getOrCreateEdge(name: String, fieldMap: FieldMap<S>): Edge<S>
    fun <S : Schema, T> updateField(edge: Edge<S>, field: Field<S, T>, value: T): Edge<S>
    fun <S : Schema> updateFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S>

    fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> createNode(name: String, fieldMap: FieldMap<S>): Node<S>?
    fun <S : Schema> createOrReplaceNode(name: String, fieldMap: FieldMap<S>): Node<S>
    fun deleteNode(name: String, withConnections: Boolean = true): Boolean
    fun deleteNode(id: NodeId, withConnections: Boolean = true): Boolean
    fun findNodeSchema(id: NodeId): Schema?
    fun <S : Schema> getOrCreateNode(name: String, fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema, T> updateField(node: Node<S>, field: Field<S, T>, value: T): Node<S>
    fun <S : Schema> updateFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S>

    fun <S : Schema> query(match: EdgeMatch<S>): Sequence<Edge<S>>
    fun <S : Schema> query(match: NodeMatch<S>): Sequence<Node<S>>

    fun <T> transaction(fn: GraphLiteDatabase.() -> T): T
}
