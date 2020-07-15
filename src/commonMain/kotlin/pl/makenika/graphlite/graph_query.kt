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

import pl.makenika.graphlite.impl.EdgesBySchema
import pl.makenika.graphlite.impl.NodesBySchema
import pl.makenika.graphlite.impl.OrderImpl
import pl.makenika.graphlite.impl.WhereImpl

interface ElementMatch<S : Schema>

interface EdgeMatch<S : Schema> : ElementMatch<S> {
    /** Travels to matching incident nodes. */
    infix fun <R : Schema> endpoints(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident source nodes. */
    infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident target nodes. */
    infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
fun <S : Schema> EdgeMatch(
    schema: S,
    where: Where<S>? = null,
    order: Order<S>? = null
): EdgeMatch<S> {
    return EdgesBySchema(schema, where, order)
}

interface NodeMatch<S : Schema> : ElementMatch<S> {
    /** Travels via matching incoming edges. */
    infix fun <R : Schema> incoming(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching outgoing edges. */
    infix fun <R : Schema> outgoing(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching edges. */
    infix fun <R : Schema> via(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels to matching neighbour nodes. */
    infix fun <R : Schema> adjacent(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching source nodes. */
    infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching target nodes. */
    infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
fun <S : Schema> NodeMatch(
    schema: S,
    where: Where<S>? = null,
    order: Order<S>? = null
): NodeMatch<S> {
    return NodesBySchema(schema, where, order)
}

interface Where<S : Schema> {
    companion object {
        fun <S : Schema> and(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.And(a, b)

        fun <S : Schema> or(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.Or(a, b)

        fun <S : Schema> handle(handle: ElementHandle): Where<S> = WhereImpl.Handle(handle)

        fun <S : Schema> handle(handleName: String): Where<S> = WhereImpl.Handle(ElementHandle(handleName))

        fun <S : Schema, V : Any, T : V?> between(
            field: IndexableScalarField<S, T>,
            start: V,
            end: V
        ): Where<S> =
            WhereImpl.Between(field, start, end)

        fun <S : Schema, T> eq(field: IndexableField<S, T>, value: T): Where<S> =
            WhereImpl.Equal(field, value)

        fun <S : Schema, V : Any, T : V?> gt(
            field: IndexableScalarField<S, T>,
            value: V
        ): Where<S> =
            WhereImpl.GreaterThan(field, value)

        fun <S : Schema, V : Any, T : V?> lt(
            field: IndexableScalarField<S, T>,
            value: V
        ): Where<S> =
            WhereImpl.LessThan(field, value)

        fun <S : Schema, T> within(field: IndexableScalarField<S, T>, values: List<T>): Where<S> =
            WhereImpl.Within(field, values)

        fun <S : Schema> fts(field: IndexableScalarField<S, String>, value: String): Where<S> =
            WhereImpl.FullText(field, value)

        fun <S : Schema, T : GeoBounds?> inside(
            field: IndexableField<S, T>,
            value: GeoBounds
        ): Where<S> =
            WhereImpl.Inside(field, value)

        fun <S : Schema, T : GeoBounds?> overlaps(
            field: IndexableField<S, T>,
            value: GeoBounds
        ): Where<S> =
            WhereImpl.Overlaps(field, value)
    }
}

interface Order<S : Schema> {
    companion object {
        fun <S : Schema> asc(field: IndexableScalarField<S, *>): Order<S> = OrderImpl.Asc(field)
        fun <S : Schema> desc(field: IndexableScalarField<S, *>): Order<S> = OrderImpl.Desc(field)
    }
}
