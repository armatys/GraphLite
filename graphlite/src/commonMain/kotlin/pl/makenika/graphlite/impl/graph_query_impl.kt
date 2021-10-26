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

import pl.makenika.graphlite.*

internal sealed class ElementMatchImpl<S : Schema> : ElementMatch<S>

internal sealed class EdgeMatchImpl<S : Schema> : ElementMatchImpl<S>(), EdgeMatch<S> {
    override fun <R : Schema> endpoints(match: NodeMatch<R>): NodeMatch<R> {
        return ConnectedNodes(this, match)
    }

    override fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R> {
        return SourceNodesViaEdges(this, match)
    }

    override fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R> {
        return TargetNodesViaEdges(this, match)
    }
}

internal sealed class NodeMatchImpl<S : Schema> : ElementMatchImpl<S>(), NodeMatch<S> {
    override fun <R : Schema> incoming(match: EdgeMatch<R>): EdgeMatch<R> {
        return TraceInEdges(this, match)
    }

    override fun <R : Schema> outgoing(match: EdgeMatch<R>): EdgeMatch<R> {
        return FollowOutEdges(this, match)
    }

    override fun <R : Schema> via(match: EdgeMatch<R>): EdgeMatch<R> {
        return ConnectedEdges(this, match)
    }

    override fun <R : Schema> adjacent(match: NodeMatch<R>): NodeMatch<R> {
        return Neighbours(this, match)
    }

    override fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R> {
        return SourceNeighbours(this, match)
    }

    override fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R> {
        return TargetNeighbours(this, match)
    }
}

internal class EdgesBySchema<S : Schema>(
    val schema: S,
    val where: Where<S>?,
    val order: Order<S>?
) : EdgeMatchImpl<S>()

internal class ConnectedNodes<E : Schema, N : Schema>(
    val edges: EdgeMatch<E>,
    val nodes: NodeMatch<N>
) : NodeMatchImpl<N>()

internal class TargetNodesViaEdges<E : Schema, N : Schema>(
    val edges: EdgeMatch<E>,
    val nodes: NodeMatch<N>
) : NodeMatchImpl<N>()

internal class SourceNodesViaEdges<E : Schema, N : Schema>(
    val edges: EdgeMatch<E>,
    val nodes: NodeMatch<N>
) : NodeMatchImpl<N>()

internal class NodesBySchema<S : Schema>(
    val schema: S,
    val where: Where<S>?,
    val order: Order<S>?
) : NodeMatchImpl<S>()

internal class FollowOutEdges<N : Schema, E : Schema>(
    val nodes: NodeMatch<N>,
    val edges: EdgeMatch<E>
) : EdgeMatchImpl<E>()

internal class TraceInEdges<N : Schema, E : Schema>(
    val nodes: NodeMatch<N>,
    val edges: EdgeMatch<E>
) : EdgeMatchImpl<E>()

internal class ConnectedEdges<N : Schema, E : Schema>(
    val nodes: NodeMatch<N>,
    val edges: EdgeMatch<E>
) : EdgeMatchImpl<E>()

internal class Neighbours<M : Schema, N : Schema>(val start: NodeMatch<M>, val end: NodeMatch<N>) :
    NodeMatchImpl<N>()

internal class TargetNeighbours<M : Schema, N : Schema>(
    val start: NodeMatch<M>,
    val target: NodeMatch<N>
) : NodeMatchImpl<N>()

internal class SourceNeighbours<M : Schema, N : Schema>(
    val start: NodeMatch<M>,
    val source: NodeMatch<N>
) : NodeMatchImpl<N>()

internal sealed class WhereImpl<S : Schema> : Where<S> {
    class And<S : Schema>(val a: Where<S>, val b: Where<S>) : WhereImpl<S>()
    class Between<S : Schema, V : Any, T>(
        val field: IndexableScalarField<S, T>,
        val start: V,
        val end: V
    ) : WhereImpl<S>()

    class Equal<S : Schema, T>(val field: IndexableField<S, T>, val value: T) : WhereImpl<S>()
    class FullText<S : Schema>(val field: IndexableScalarField<S, String>, val value: String) :
        WhereImpl<S>()

    class GreaterThan<S : Schema, V : Any, T>(val field: IndexableScalarField<S, T>, val value: V) :
        WhereImpl<S>()

    class Inside<S : Schema, T : GeoBounds?>(
        val field: IndexableField<S, T>,
        val value: GeoBounds
    ) : WhereImpl<S>()

    class LessThan<S : Schema, V : Any, T>(val field: IndexableScalarField<S, T>, val value: V) :
        WhereImpl<S>()

    class Handle<S : Schema>(val handle: ElementHandle) : WhereImpl<S>()
    class Or<S : Schema>(val a: Where<S>, val b: Where<S>) : WhereImpl<S>()
    class Overlaps<S : Schema, T : GeoBounds?>(
        val field: IndexableField<S, T>,
        val value: GeoBounds
    ) : WhereImpl<S>()

    class Within<S : Schema, T>(val field: IndexableScalarField<S, T>, val values: List<T>) :
        WhereImpl<S>()
}

internal sealed class OrderImpl<S : Schema>(val field: IndexableScalarField<S, *>) : Order<S> {
    class Asc<S : Schema>(field: IndexableScalarField<S, *>) : OrderImpl<S>(field)
    class Desc<S : Schema>(field: IndexableScalarField<S, *>) : OrderImpl<S>(field)
}
