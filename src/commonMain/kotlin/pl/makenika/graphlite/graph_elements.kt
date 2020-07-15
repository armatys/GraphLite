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

interface Handle {
    val value: String
}

interface SchemaHandle : Handle
interface FieldHandle : Handle
interface ElementHandle : Handle
interface EdgeHandle : ElementHandle
interface NodeHandle : ElementHandle

@Suppress("FunctionName")
fun SchemaHandle(value: String): SchemaHandle = SchemaHandleImpl(value)

@Suppress("FunctionName")
fun FieldHandle(value: String): FieldHandle = FieldHandleImpl(value)

@Suppress("FunctionName")
fun ElementHandle(value: String): ElementHandle = ElementHandleImpl(value)

@Suppress("FunctionName")
fun EdgeHandle(value: String): EdgeHandle = EdgeHandleImpl(value)

@Suppress("FunctionName")
fun NodeHandle(value: String): NodeHandle = NodeHandleImpl(value)

private inline class SchemaHandleImpl(override val value: String) : SchemaHandle
private inline class FieldHandleImpl(override val value: String) : FieldHandle
private inline class ElementHandleImpl(override val value: String) : ElementHandle
private inline class EdgeHandleImpl(override val value: String) : EdgeHandle
private inline class NodeHandleImpl(override val value: String) : NodeHandle

data class Connection(
    val edgeHandle: EdgeHandle,
    val nodeHandle: NodeHandle,
    val outgoing: Boolean?
)

enum class ElementType(internal val code: String) {
    Edge("e"), Node("n")
}

interface GraphElement<S : Schema> {
    val handle: ElementHandle
    val fieldMap: FieldMap<S>
    val type: ElementType
}

data class Edge<S : Schema>(
    override val handle: EdgeHandle,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Edge
}

data class Node<S : Schema>(
    override val handle: NodeHandle,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Node
}
