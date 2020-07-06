package pl.makenika.graphlite.sql

sealed class TransactionResult<T> {
    fun getOrDefault(t: T): T {
        return when (this) {
            is Ok -> value
            is Fail -> t
        }
    }

    fun getOrThrow(): T {
        return when (this) {
            is Ok -> value
            is Fail -> throw error
        }
    }

    class Ok<T>(val value: T) : TransactionResult<T>()
    class Fail<T>(val error: Throwable) : TransactionResult<T>()
}
