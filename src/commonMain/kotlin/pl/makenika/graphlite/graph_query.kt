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

public interface ElementMatch<S : Schema>

public interface EdgeMatch<S : Schema> : ElementMatch<S> {
    /** Travels to matching incident nodes. */
    public infix fun <R : Schema> endpoints(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident source nodes. */
    public infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident target nodes. */
    public infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
public fun <S : Schema> EdgeMatch(
    schema: S,
    where: Where<S>? = null,
    order: Order<S>? = null
): EdgeMatch<S> {
    return EdgesBySchema(schema, where, order)
}

public interface NodeMatch<S : Schema> : ElementMatch<S> {
    /** Travels via matching incoming edges. */
    public infix fun <R : Schema> incoming(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching outgoing edges. */
    public infix fun <R : Schema> outgoing(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching edges. */
    public infix fun <R : Schema> via(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels to matching neighbour nodes. */
    public infix fun <R : Schema> adjacent(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching source nodes. */
    public infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching target nodes. */
    public infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
public fun <S : Schema> NodeMatch(
    schema: S,
    where: Where<S>? = null,
    order: Order<S>? = null
): NodeMatch<S> {
    return NodesBySchema(schema, where, order)
}

public interface Where<S : Schema> {
    public companion object {
        public fun <S : Schema> and(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.And(a, b)

        public fun <S : Schema> or(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.Or(a, b)

        public fun <S : Schema> handle(handle: ElementHandle): Where<S> = WhereImpl.Handle(handle)

        public fun <S : Schema> handle(handleName: String): Where<S> =
            WhereImpl.Handle(ElementHandle(handleName))

        public fun <S : Schema, V : Any, T : V?> between(
            field: IndexableScalarField<S, T>,
            start: V,
            end: V
        ): Where<S> =
            WhereImpl.Between(field, start, end)

        public fun <S : Schema, T> eq(field: IndexableField<S, T>, value: T): Where<S> =
            WhereImpl.Equal(field, value)

        public fun <S : Schema, V : Any, T : V?> gt(
            field: IndexableScalarField<S, T>,
            value: V
        ): Where<S> =
            WhereImpl.GreaterThan(field, value)

        public fun <S : Schema, V : Any, T : V?> lt(
            field: IndexableScalarField<S, T>,
            value: V
        ): Where<S> =
            WhereImpl.LessThan(field, value)

        public fun <S : Schema, T> within(
            field: IndexableScalarField<S, T>,
            values: List<T>
        ): Where<S> =
            WhereImpl.Within(field, values)

        public fun <S : Schema> fts(
            field: IndexableScalarField<S, String>,
            value: String
        ): Where<S> =
            WhereImpl.FullText(field, value)

        public fun <S : Schema, T : GeoBounds?> inside(
            field: IndexableField<S, T>,
            value: GeoBounds
        ): Where<S> =
            WhereImpl.Inside(field, value)

        public fun <S : Schema, T : GeoBounds?> overlaps(
            field: IndexableField<S, T>,
            value: GeoBounds
        ): Where<S> =
            WhereImpl.Overlaps(field, value)
    }
}

public interface Order<S : Schema> {
    public companion object {
        public fun <S : Schema> asc(field: IndexableScalarField<S, *>): Order<S> =
            OrderImpl.Asc(field)

        public fun <S : Schema> desc(field: IndexableScalarField<S, *>): Order<S> =
            OrderImpl.Desc(field)
    }
}
