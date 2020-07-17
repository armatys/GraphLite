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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import pl.makenika.graphlite.*
import pl.makenika.graphlite.impl.GraphSqlUtils.getFieldId
import pl.makenika.graphlite.sql.*

internal class QueryEngine(private val driver: SqliteDriver) {
    fun <S : Schema> performQuery(match: ElementMatchImpl<S>): Pair<S, Flow<Pair<String, String>>> {
        val querySpec = performQueryMatch(match)
        val query = buildSelect(
            "SELECT Element.id, Element.handle FROM Element",
            join = querySpec.join,
            where = querySpec.where,
            orderBy = querySpec.orderBy
        )

        val idToHandleValueFlow: Flow<Pair<String, String>> = flow {
            driver.query(query, querySpec.whereBindings.toTypedArray())
                .use { cursor ->
                    while (cursor.moveToNext()) {
                        val id = cursor.getString("id")
                        val handle = cursor.getString("handle")
                        emit(id to handle)
                    }
                }
        }
        return querySpec.schema to idToHandleValueFlow
    }

    private fun <S : Schema> performQueryMatch(match: ElementMatch<S>): SqlQuerySpec<S> {
        return when (match) {
            is ElementMatchImpl<S> -> performQueryMatch(match)
            else -> error("Unknown ElementMatch subclass: $match")
        }
    }

    private fun <S : Schema> performQueryMatch(match: ElementMatchImpl<S>): SqlQuerySpec<S> {
        return when (match) {
            is EdgesBySchema<S> -> getEdgesBySchema(match)
            is NodesBySchema<S> -> getNodesBySchema(match)
            is FollowOutEdges<*, S> -> getFollowOutEdges(match)
            is TraceInEdges<*, S> -> getTraceInEdges(match)
            is ConnectedEdges<*, S> -> getConnectedEdges(match)
            is ConnectedNodes<*, S> -> getConnectedNodes(match)
            is SourceNodesViaEdges<*, S> -> getSourceNodesViaEdges(match)
            is TargetNodesViaEdges<*, S> -> getTargetNodesViaEdges(match)
            is Neighbours<*, S> -> getNeighbours(match)
            is TargetNeighbours<*, S> -> getTargetNeighbours(match)
            is SourceNeighbours<*, S> -> getSourceNeighbours(match)
        }
    }

    private fun <S : Schema> getEdgesBySchema(match: EdgesBySchema<S>): SqlQuerySpec<S> {
        return getElementsBySchema(ElementType.Edge, match.schema, match.where, match.order)
    }

    private fun <S : Schema> getNodesBySchema(match: NodesBySchema<S>): SqlQuerySpec<S> {
        return getElementsBySchema(ElementType.Node, match.schema, match.where, match.order)
    }

    private fun <S : Schema> getElementsBySchema(
        elementType: ElementType,
        schema: S,
        where: Where<S>?,
        order: Order<S>?
    ): SqlQuerySpec<S> {
        val bindings = mutableListOf<String>()
        val whereClauses = StringBuilder()

        whereClauses.append("schemaId = (SELECT id FROM Schema WHERE handle = ? AND version = ? LIMIT 1) AND type = ?")
        bindings.add(schema.schemaHandle.value)
        bindings.add(schema.schemaVersion.toString())
        bindings.add(elementType.code)

        where?.let {
            val (conditions, conditionBindings) = buildWhere(schema, it)
            whereClauses.append(" AND $conditions")
            bindings.addAll(conditionBindings)
        }

        val (join, orderBy) = if (order != null) {
            buildOrder(schema, order)
        } else {
            null to null
        }

        return SqlQuerySpec(
            schema,
            join = join,
            where = whereClauses.toString(),
            whereBindings = bindings,
            orderBy = orderBy
        )
    }

    private fun <S : Schema> getFollowOutEdges(match: FollowOutEdges<*, S>): SqlQuerySpec<S> {
        return getEdgesFromNodes(match.nodes, match.edges, outgoing = true)
    }

    private fun <S : Schema> getTraceInEdges(match: TraceInEdges<*, S>): SqlQuerySpec<S> {
        return getEdgesFromNodes(match.nodes, match.edges, outgoing = false)
    }

    private fun <S : Schema> getConnectedEdges(match: ConnectedEdges<*, S>): SqlQuerySpec<S> {
        return getEdgesFromNodes(match.nodes, match.edges, outgoing = null)
    }

    private fun <S : Schema> getEdgesFromNodes(
        nodes: NodeMatch<out Schema>,
        edges: EdgeMatch<S>,
        outgoing: Boolean?
    ): SqlQuerySpec<S> {
        val bindings = mutableListOf<String>()
        val whereClauses = StringBuilder()

        val nodeSpec = performQueryMatch(nodes)
        val edgeSpec = performQueryMatch(edges)

        val outgoingClause = when (outgoing) {
            true -> "AND outgoing = 1"
            false -> "AND outgoing = 0"
            null -> ""
        }

        val selectElementHandle = buildSelect(
            "SELECT handle FROM Element",
            nodeSpec.join,
            nodeSpec.where,
            nodeSpec.orderBy
        )
        whereClauses.append("handle IN (SELECT edgeHandle FROM Connection WHERE nodeHandle IN ($selectElementHandle) $outgoingClause) ")
        bindings.addAll(nodeSpec.whereBindings)

        whereClauses.append("AND ${edgeSpec.where}")
        bindings.addAll(edgeSpec.whereBindings)

        return SqlQuerySpec(
            edgeSpec.schema,
            join = edgeSpec.join,
            where = whereClauses.toString(),
            whereBindings = bindings,
            orderBy = edgeSpec.orderBy
        )
    }

    private fun <S : Schema> getConnectedNodes(match: ConnectedNodes<*, S>): SqlQuerySpec<S> {
        return getNodesFromEdges(match.edges, match.nodes, null)
    }

    private fun <S : Schema> getSourceNodesViaEdges(match: SourceNodesViaEdges<*, S>): SqlQuerySpec<S> {
        return getNodesFromEdges(match.edges, match.nodes, true)
    }

    private fun <S : Schema> getTargetNodesViaEdges(match: TargetNodesViaEdges<*, S>): SqlQuerySpec<S> {
        return getNodesFromEdges(match.edges, match.nodes, false)
    }

    private fun <S : Schema> getNodesFromEdges(
        edges: EdgeMatch<out Schema>,
        nodes: NodeMatch<S>,
        outgoing: Boolean?
    ): SqlQuerySpec<S> {
        val bindings = mutableListOf<String>()
        val whereClauses = StringBuilder()

        val edgeSpec = performQueryMatch(edges)
        val nodeSpec = performQueryMatch(nodes)

        val outgoingClause = when (outgoing) {
            true -> "AND outgoing = 1"
            false -> "AND outgoing = 0"
            null -> ""
        }

        val selectElementHandle = buildSelect(
            "SELECT handle FROM Element",
            edgeSpec.join,
            edgeSpec.where,
            edgeSpec.orderBy
        )
        whereClauses.append("handle IN (SELECT nodeHandle FROM Connection WHERE edgeHandle IN ($selectElementHandle) $outgoingClause) ")
        bindings.addAll(edgeSpec.whereBindings)

        whereClauses.append("AND ${nodeSpec.where}")
        bindings.addAll(nodeSpec.whereBindings)

        return SqlQuerySpec(
            nodeSpec.schema,
            join = nodeSpec.join,
            where = whereClauses.toString(),
            whereBindings = bindings,
            orderBy = nodeSpec.orderBy
        )
    }

    private fun <S : Schema> getNeighbours(match: Neighbours<*, S>): SqlQuerySpec<S> {
        return getNodeNeighbours(match.start, match.end, null)
    }

    private fun <S : Schema> getSourceNeighbours(match: SourceNeighbours<*, S>): SqlQuerySpec<S> {
        return getNodeNeighbours(match.start, match.source, false)
    }

    private fun <S : Schema> getTargetNeighbours(match: TargetNeighbours<*, S>): SqlQuerySpec<S> {
        return getNodeNeighbours(match.start, match.target, true)
    }

    private fun <S : Schema> getNodeNeighbours(
        from: NodeMatch<out Schema>,
        to: NodeMatch<S>,
        targets: Boolean?
    ): SqlQuerySpec<S> {
        val bindings = mutableListOf<String>()
        val whereClauses = StringBuilder()

        val fromSpec = performQueryMatch(from)
        val toSpec = performQueryMatch(to)

        val outgoingClause = when (targets) {
            true -> "AND outgoing = 0"
            false -> "AND outgoing = 1"
            null -> ""
        }

        val selectElementHandle = buildSelect(
            "SELECT handle FROM Element",
            fromSpec.join,
            fromSpec.where,
            fromSpec.orderBy
        )
        whereClauses.append("handle IN (SELECT nodeHandle FROM Connection, (SELECT id, edgeHandle FROM Connection WHERE nodeHandle IN ($selectElementHandle)) AS C WHERE Connection.edgeHandle = C.edgeHandle AND Connection.id != C.id $outgoingClause)")
        bindings.addAll(fromSpec.whereBindings)

        whereClauses.append(" AND ${toSpec.where}")
        bindings.addAll(toSpec.whereBindings)

        return SqlQuerySpec(
            toSpec.schema,
            join = toSpec.join,
            where = whereClauses.toString(),
            whereBindings = bindings,
            orderBy = toSpec.orderBy
        )
    }

    private fun buildSelect(
        selectIntro: String,
        join: String?,
        where: String,
        orderBy: String?
    ): String {
        val builder = mutableListOf<String>()
        builder.add(selectIntro)

        join?.let {
            builder.add("INNER JOIN")
            builder.add(it)
        }

        builder.add("WHERE")
        builder.add(where)

        orderBy?.let {
            builder.add("ORDER BY")
            builder.add(it)
        }

        return builder.joinToString(" ")
    }

    //================== WHERE filtering

    private fun <S : Schema> buildWhere(schema: S, where: Where<S>): Pair<String, List<String>> {
        return when (val whereImpl =
            where as? WhereImpl ?: error("Unknown Where subclass: $where")) {
            is WhereImpl.And<S> -> whereAnd(schema, whereImpl)
            is WhereImpl.Between<S, *, *> -> whereBetween(schema, whereImpl)
            is WhereImpl.Equal<S, *> -> whereEqualValue(schema, whereImpl)
            is WhereImpl.FullText<S> -> whereFullText(schema, whereImpl)
            is WhereImpl.GreaterThan<S, *, *> -> whereGreaterThan(schema, whereImpl)
            is WhereImpl.Inside<S, *> -> whereInside(schema, whereImpl)
            is WhereImpl.LessThan<S, *, *> -> whereLessThan(schema, whereImpl)
            is WhereImpl.Handle<S> -> whereHandle(whereImpl)
            is WhereImpl.Or<S> -> whereOr(schema, whereImpl)
            is WhereImpl.Overlaps<S, *> -> whereOverlaps(schema, whereImpl)
            is WhereImpl.Within<S, *> -> whereWithin(schema, whereImpl)
        }
    }

    private fun <S : Schema> whereAnd(
        schema: S,
        where: WhereImpl.And<S>
    ): Pair<String, List<String>> {
        val (aConditions, aBindings) = buildWhere(schema, where.a)
        val (bConditions, bBindings) = buildWhere(schema, where.b)
        return Pair(
            "($aConditions) AND ($bConditions)",
            aBindings + bBindings
        )
    }

    private fun <S : Schema> whereBetween(
        schema: S,
        where: WhereImpl.Between<S, *, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE value BETWEEN ? AND ?)",
            listOf(where.start.toString(), where.end.toString())
        )
    }

    private fun <S : Schema> whereEqualValue(
        schema: S,
        where: WhereImpl.Equal<S, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        val value = where.value

        return if (value is GeoBounds?) {
            if (value == null) {
                Pair(
                    "Element.id IN (SELECT elementId FROM $valueTableName WHERE minLat IS NULL)",
                    emptyList()
                )
            } else {
                Pair(
                    "Element.id IN (SELECT elementId FROM $valueTableName WHERE minLat = ? AND maxLat = ? AND minLon = ? AND maxLon = ?)",
                    listOf(
                        value.minLat.toString(),
                        value.maxLat.toString(),
                        value.minLon.toString(),
                        value.maxLon.toString()
                    )
                )
            }
        } else {
            if (value == null) {
                Pair(
                    "Element.id IN (SELECT elementId FROM $valueTableName WHERE value IS NULL)",
                    emptyList()
                )
            } else {
                Pair(
                    "Element.id IN (SELECT elementId FROM $valueTableName WHERE value = ?)",
                    listOf(value.toString())
                )
            }
        }
    }

    private fun <S : Schema> whereFullText(
        schema: S,
        where: WhereImpl.FullText<S>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        val ftsTableName = getFtsTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE rowid IN (SELECT rowid FROM $ftsTableName WHERE value MATCH ?))",
            listOf(where.value)
        )
    }

    private fun <S : Schema> whereGreaterThan(
        schema: S,
        where: WhereImpl.GreaterThan<S, *, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE value > ?)",
            listOf(where.value.toString())
        )
    }

    private fun <S : Schema> whereInside(
        schema: S,
        where: WhereImpl.Inside<S, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        val geoTableName = getRTreeTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE rowid IN (SELECT rowid FROM $geoTableName WHERE minLat >= ? AND maxLat <= ? AND minLon >= ? AND maxLon <= ?))",
            listOf(
                where.value.minLat.toString(),
                where.value.maxLat.toString(),
                where.value.minLon.toString(),
                where.value.maxLon.toString()
            )
        )
    }

    private fun <S : Schema> whereLessThan(
        schema: S,
        where: WhereImpl.LessThan<S, *, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE value < ?)",
            listOf(where.value.toString())
        )
    }

    private fun <S : Schema> whereHandle(where: WhereImpl.Handle<S>): Pair<String, List<String>> {
        return Pair("handle = ?", listOf(where.handle.value))
    }

    private fun <S : Schema> whereOr(
        schema: S,
        where: WhereImpl.Or<S>
    ): Pair<String, List<String>> {
        val (aConditions, aBindings) = buildWhere(schema, where.a)
        val (bConditions, bBindings) = buildWhere(schema, where.b)
        return Pair(
            "($aConditions) OR ($bConditions)",
            aBindings + bBindings
        )
    }

    private fun <S : Schema> whereOverlaps(
        schema: S,
        where: WhereImpl.Overlaps<S, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        val geoTableName = getRTreeTableName(fieldId)
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE rowid IN (SELECT rowid FROM $geoTableName WHERE minLat <= ? AND maxLat >= ? AND minLon <= ? AND maxLon >= ?))",
            listOf(
                where.value.maxLat.toString(),
                where.value.minLat.toString(),
                where.value.maxLon.toString(),
                where.value.minLon.toString()
            )
        )
    }

    private fun <S : Schema> whereWithin(
        schema: S,
        where: WhereImpl.Within<S, *>
    ): Pair<String, List<String>> {
        val fieldId = getFieldId(driver, where.field, schema)
        val valueTableName = getFieldValueTableName(fieldId)
        val placeholders = where.values.joinToString(", ") { "?" }
        return Pair(
            "Element.id IN (SELECT elementId FROM $valueTableName WHERE value IN ($placeholders))",
            where.values.map { it.toString() }
        )
    }

    //================== ORDER BY

    private fun <S : Schema> buildOrder(schema: S, order: Order<S>): Pair<String, String> {
        val orderImpl = order as? OrderImpl ?: error("Unknown Order subclass: ${order::class}.")
        val orderOperator = when (orderImpl) {
            is OrderImpl.Asc -> "ASC"
            is OrderImpl.Desc -> "DESC"
        }
        return buildOrder(schema, orderImpl.field, orderOperator)
    }

    private fun <S : Schema> buildOrder(
        schema: S,
        field: IndexableScalarField<S, *>,
        orderOperator: String
    ): Pair<String, String> {
        val fieldId = getFieldId(driver, field, schema)
        val tableName = getFieldValueTableName(fieldId)
        val columnName = "value"
        val join = "$tableName ON $tableName.elementId = Element.id"
        val orderBy = "$tableName.$columnName $orderOperator"
        return Pair(join, orderBy)
    }
}
