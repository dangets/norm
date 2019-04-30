package com.dangets.norm

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.Test

internal class ResultTest {
    @Test
    fun `test ok unwrap`() {
        assertThat(Result.Ok(10).unwrap())
                .isEqualTo(10)
    }

    @Test
    fun `test ok map`() {
        val r = Result.Ok(10)
        assertThat(r.map { it.toString() })
                .isEqualTo(Result.Ok("10"))
    }

    @Test
    fun `test when`() {
        val r: Result<Int, String> = Result.Ok(10)
        when (r) {
            is Result.Ok -> assertThat(r.value).isEqualTo(10)
            is Result.Err -> fail("shouldn't be here: ${r.error}")
        }
    }
}

