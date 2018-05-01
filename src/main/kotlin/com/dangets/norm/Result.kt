package com.dangets.norm


sealed class Result<ValT, ErrT> {
    data class Ok<ValT, ErrT>(val value: ValT) : Result<ValT, ErrT>()
    data class Err<ValT, ErrT>(val error: ErrT) : Result<ValT, ErrT>()

    fun unwrap(): ValT = when (this) {
        is Ok -> this.value
        is Err -> throw NoSuchElementException("unwrap called on a Result.Err")
    }

    //companion object {
    //    fun <ValT, ErrT> ok(value: ValT) = Result.Ok<ValT, ErrT>(value)
    //    fun <ValT, ErrT> error(error: ErrT) = Result.Err<ValT, ErrT>(error)
    //}
}
