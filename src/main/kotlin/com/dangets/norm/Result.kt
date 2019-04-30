package com.dangets.norm

import java.util.*


sealed class Result<out V, out E> {
    data class Ok<V>(val value: V) : Result<V, Nothing>()
    data class Err<E>(val error: E) : Result<Nothing, E>()

    // DG: unsure if I want to support Iterable ...
    //override fun iterator(): Iterator<V> = when (this) {
    //    is Ok -> Collections.singleton(this.value).iterator()
    //    is Result.Err -> Collections.emptyIterator()
    //}

    fun unwrap(): V = when (this) {
        is Ok -> this.value
        is Err -> throw NoSuchElementException("unwrap called on a Result.Err")
    }
}

fun <V, U, E> Result<V, E>.map(mapFn: (V) -> U): Result<U, E> =
        when (this) {
            is Result.Ok -> Result.Ok(mapFn(this.value))
            is Result.Err -> this
        }

fun <V, U, E> Result<V, E>.flatMap(mapFn: (V) -> Result<U, E>): Result<U, E> =
        when (this) {
            is Result.Ok -> mapFn(this.value)
            is Result.Err -> this
        }

fun <V, E> Result<V, E>.or(other: V): Result<V, E> =
        when (this) {
            is Result.Ok -> this
            is Result.Err -> Result.Ok(other)
        }
