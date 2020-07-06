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

interface GraphId

interface ConnectionId : GraphId
interface FieldId : GraphId

interface ElementId : GraphId
interface EdgeId : ElementId
interface NodeId : ElementId

internal inline class ConnectionIdImpl(private val uuid: String) : ConnectionId {
    override fun toString(): String {
        return uuid
    }
}

internal inline class EdgeIdImpl(private val uuid: String) : EdgeId {
    override fun toString(): String {
        return uuid
    }
}

internal inline class FieldIdImpl(private val uuid: String) : FieldId {
    override fun toString(): String {
        return uuid
    }
}

internal inline class NodeIdImpl(private val uuid: String) : NodeId {
    override fun toString(): String {
        return uuid
    }
}

data class Connection(
    val id: ConnectionId,
    val edgeName: String,
    val nodeName: String,
    val outgoing: Boolean?
)

enum class ElementType(internal val code: String) {
    Edge("e"), Node("n")
}

interface GraphElement<S : Schema> {
    val id: ElementId
    val name: String
    val fieldMap: FieldMap<S>
    val type: ElementType
}

data class Edge<S : Schema>(
    override val id: EdgeId,
    override val name: String,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Edge
}

data class Node<S : Schema>(
    override val id: NodeId,
    override val name: String,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Node
}
