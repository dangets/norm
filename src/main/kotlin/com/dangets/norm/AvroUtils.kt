package com.dangets.norm

fun toAvroSchema(fm: FileModel): String {
    val columns = when (fm) {
        is FixedWidthFileModel -> fm.columns.joinToString(",") { toAvroColumn(it) }
        is CsvFileModel -> fm.columns.joinToString(",") { toAvroColumn(it) }
    }

    return """{ "namespace": "com.dangets", "type": "record", "name": "FileModelX", "fields": [$columns] }"""
}

private fun toAvroColumn(col: CsvFileModel.Column): String {
    return """{"name": "${col.name}", "type": ${toAvroColType(col.type)}}"""
}
private fun toAvroColumn(col: FixedWidthFileModel.Column): String {
    return """{"name": "${col.name}", "type": ${toAvroColType(col.type)}}"""
}

private fun toAvroColType(colType: ColumnType): String {
    val typeString = when (colType) {
        is ColumnTypeString -> "string"
        is ColumnTypeInt -> "int"
        is ColumnTypeFloat -> "double"
        is ColumnTypeDate -> """{"type":"int", "logicalType":"date"}"""
    }
    if (colType.isNullable)
        return """["null", "$typeString"]"""
    return "\"$typeString\""
}


fun main(args: Array<String>) {
    val csv = CsvFileModel(
            numHeaderLines = 1,
            numFooterLines = 0,
            delimiter = ",",
            columns = listOf(
                    CsvFileModel.Column(name = "accountId", type = ColumnTypeInt(), isIdentifier = false)
            )
    )

    toAvroSchema(csv).let { println(it) }
}