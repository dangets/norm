package com.dangets.norm

import java.time.Instant
import java.time.LocalDate

data class VersionedFileModel(
        val fileId: Long,
        val versionId: Long,
        val active: Boolean,
        val activeReconDate: LocalDate,
        val inactiveReconDate: LocalDate?,
        val tsCreated: Instant,
        val createdBy: String,
        val fileModel: FileModel
)

sealed class FileModel {
    abstract val numHeaderLines: Int
    abstract val numFooterLines: Int
}

data class CsvFileModel(override val numHeaderLines: Int,
                        override val numFooterLines: Int,
                        val delimiter: String,
                        val columns: List<Column>) : FileModel()
{
    init {
        require(numHeaderLines >= 0) { "numHeaderLines cannot be negative: $numHeaderLines" }
        require(numFooterLines >= 0) { "numHeaderLines cannot be negative: $numFooterLines" }
        require(delimiter.isNotEmpty()) { "delimiter cannot be empty" }
        require(columns.isNotEmpty()) { "columns must not be empty" }
    }

    data class Column(val name: String, val type: ColumnType, val isIdentifier: Boolean) {
        init {
            require(name.isNotBlank()) { "name cannot be blank" }
        }
    }
}

data class FixedWidthFileModel(override val numHeaderLines: Int,
                        override val numFooterLines: Int,
                        val columns: List<Column>) : FileModel()
{
    init {
        require(numHeaderLines >= 0) { "numHeaderLines cannot be negative: $numHeaderLines" }
        require(numFooterLines >= 0) { "numHeaderLines cannot be negative: $numFooterLines" }
        require(columns.isNotEmpty()) { "columns must not be empty" }
    }

    data class Column(val name: String, val offset: Int, val width: Int,
                      val type: ColumnType, val isIdentifier: Boolean) {
        init {
            require(name.isNotBlank()) { "name cannot be blank" }
            require(offset >= 0) { "invalid offset '$offset'" }
            require(width >= 0) { "invalid width '$width'" }
        }
    }
}

sealed class ColumnType {
    abstract val nullValues: List<String>
    val isNullable by lazy { nullValues.isNotEmpty() }
}
data class ColumnTypeString(override val nullValues: List<String> = listOf()) : ColumnType()
data class ColumnTypeInt(val format: String = "",
                         override val nullValues: List<String> = listOf()): ColumnType()
data class ColumnTypeFloat(val format: String = "",
                           override val nullValues: List<String> = listOf()): ColumnType()
data class ColumnTypeDate(val format: String = "yyyy-MM-dd",
                          override val nullValues: List<String> = listOf()): ColumnType()



fun main(args: Array<String>) {
    val csv = CsvFileModel(
            numHeaderLines = 1,
            numFooterLines = 0,
            delimiter = ",",
            columns = listOf(
                    CsvFileModel.Column(name = "accountId", type = ColumnTypeInt(), isIdentifier = false)
            )
    )

    val fixed = FixedWidthFileModel(
            numHeaderLines = 1,
            numFooterLines = 0,
            columns = listOf(
                    FixedWidthFileModel.Column("accountId", 0, 12, ColumnTypeInt(), isIdentifier = false)
            )
    )

    println(csv)
    println(fixed)
}

