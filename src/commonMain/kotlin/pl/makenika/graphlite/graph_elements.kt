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
