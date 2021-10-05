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

import kotlin.jvm.JvmInline

public interface Handle {
    public val value: String
}

public interface SchemaHandle : Handle
public interface FieldHandle : Handle
public interface ElementHandle : Handle
public interface EdgeHandle : ElementHandle
public interface NodeHandle : ElementHandle

@Suppress("FunctionName")
public fun SchemaHandle(value: String): SchemaHandle = SchemaHandleImpl(value)

@Suppress("FunctionName")
public fun FieldHandle(value: String): FieldHandle = FieldHandleImpl(value)

@Suppress("FunctionName")
public fun ElementHandle(value: String): ElementHandle = ElementHandleImpl(value)

@Suppress("FunctionName")
public fun EdgeHandle(value: String): EdgeHandle = EdgeHandleImpl(value)

@Suppress("FunctionName")
public fun NodeHandle(value: String): NodeHandle = NodeHandleImpl(value)

@JvmInline
private value class SchemaHandleImpl(override val value: String) : SchemaHandle

@JvmInline
private value class FieldHandleImpl(override val value: String) : FieldHandle

@JvmInline
private value class ElementHandleImpl(override val value: String) : ElementHandle

@JvmInline
private value class EdgeHandleImpl(override val value: String) : EdgeHandle

@JvmInline
private value class NodeHandleImpl(override val value: String) : NodeHandle

public data class Connection(
    val edgeHandle: EdgeHandle,
    val nodeHandle: NodeHandle,
    val outgoing: Boolean?
)

public enum class ElementType(internal val code: String) {
    Edge("e"), Node("n")
}

public interface GraphElement<S : Schema> {
    public val handle: ElementHandle
    public val fieldMap: FieldMap<S>
    public val type: ElementType
}

public data class Edge<S : Schema>(
    override val handle: EdgeHandle,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Edge
}

public data class Node<S : Schema>(
    override val handle: NodeHandle,
    override val fieldMap: FieldMap<S>
) : FieldMap<S> by fieldMap, GraphElement<S> {
    override val type: ElementType = ElementType.Node
}
