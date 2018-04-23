package com.dangets.norm.ws

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.afterburner.AfterburnerModule


@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes(
    Type(value = CsvFileModelDto::class, name = "csv"),
    Type(value = FixedWidthFileModelDto::class, name = "fixed")
)
sealed class FileModelDto {
    abstract var numHeaderLines: Int
    abstract var numFooterLines: Int
}

data class CsvFileModelDto(override var numHeaderLines: Int = 0,
                           override var numFooterLines: Int = 0,
                           var delimiter: String = ",",
                           var columns: List<Column> = listOf()) : FileModelDto() {

    data class Column(var name: String = "undefined",
                      var type: ColumnTypeDto = ColumnTypeStringDto(),
                      var isIdentifier: Boolean? = null)
}

data class FixedWidthFileModelDto(override var numHeaderLines: Int = 0,
                                  override var numFooterLines: Int = 0,
                                  var columns: List<Column> = listOf()) : FileModelDto() {

    data class Column(var name: String = "undefined",
                      var offset: Int = 0,
                      var width: Int = 0,
                      var type: ColumnTypeDto = ColumnTypeStringDto(),
                      var isIdentifier: Boolean? = null)
}

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes(
        Type(value = ColumnTypeStringDto::class, name = "string"),
        Type(value = ColumnTypeIntDto::class, name = "int"),
        Type(value = ColumnTypeFloatDto::class, name = "float"),
        Type(value = ColumnTypeDateDto::class, name = "date")
)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
sealed class ColumnTypeDto {
    abstract val nullValues: List<String>
}
data class ColumnTypeStringDto(override val nullValues: List<String> = listOf()) : ColumnTypeDto()
data class ColumnTypeIntDto(val format: String = "", override val nullValues: List<String> = listOf()) : ColumnTypeDto()
data class ColumnTypeFloatDto(val format: String = "", override val nullValues: List<String> = listOf()) : ColumnTypeDto()
data class ColumnTypeDateDto(val format: String = "", override val nullValues: List<String> = listOf()) : ColumnTypeDto()


fun main(args: Array<String>) {
    val csv = CsvFileModelDto(
            numHeaderLines = 1,
            numFooterLines = 0,
            delimiter = ",",
            columns = listOf(
                    //CsvFileModelDto.Column(name = "accountId", type = ColumnTypeIntDto(), isIdentifier = true),
                    //CsvFileModelDto.Column(name = "maturityDate", type = ColumnTypeDateDto(), isIdentifier = false)
                    CsvFileModelDto.Column().apply { name = "accountId"; type = ColumnTypeIntDto(); isIdentifier = true },
                    CsvFileModelDto.Column().apply { name = "maturityDate"; type = ColumnTypeDateDto(); isIdentifier = false }
            )
    )

    println(csv)

    val om = ObjectMapper().apply {
        registerModule(AfterburnerModule())

        setSerializationInclusion(JsonInclude.Include.NON_NULL)
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        configure(SerializationFeature.INDENT_OUTPUT, true)
    }

    val csvJson = om.writeValueAsString(csv)

    val csv2 = om.readValue(csvJson, FileModelDto::class.java)
    println(csv2)

    println(csvJson)
}
