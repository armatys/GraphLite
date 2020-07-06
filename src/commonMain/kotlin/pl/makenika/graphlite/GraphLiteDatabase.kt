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
    fun <S : Schema> getOrCreateEdge(schema: S, name: String, fieldMap: () -> FieldMap<S>): Edge<S>
    fun <S : Schema, T> updateField(edge: Edge<S>, field: Field<S, T>, value: T): Edge<S>
    fun <S : Schema> updateFields(edge: Edge<*>, fieldMap: FieldMap<S>): Edge<S>

    fun <S : Schema> createNode(fieldMap: FieldMap<S>): Node<S>
    fun <S : Schema> createNode(name: String, fieldMap: FieldMap<S>): Node<S>?
    fun <S : Schema> createOrReplaceNode(name: String, fieldMap: FieldMap<S>): Node<S>
    fun deleteNode(name: String, withConnections: Boolean = true): Boolean
    fun deleteNode(id: NodeId, withConnections: Boolean = true): Boolean
    fun findNodeSchema(id: NodeId): Schema?
    fun <S : Schema> getOrCreateNode(schema: S, name: String, fieldMap: () -> FieldMap<S>): Node<S>
    fun <S : Schema, T> updateField(node: Node<S>, field: Field<S, T>, value: T): Node<S>
    fun <S : Schema> updateFields(node: Node<*>, fieldMap: FieldMap<S>): Node<S>

    fun <S : Schema> query(match: EdgeMatch<S>): Sequence<Edge<S>>
    fun <S : Schema> query(match: NodeMatch<S>): Sequence<Node<S>>

    fun <T> transaction(fn: GraphLiteDatabase.() -> T): T
}
