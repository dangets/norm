package com.dangets.norm.db

import com.dangets.norm.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDate
import javax.sql.DataSource

interface FileModelDao {
    fun getFileModel(versionId: Int): VersionedFileModel?

    fun getActiveFileModel(fileId: Int, date: LocalDate): VersionedFileModel?
}

class FileModelDaoImpl(private val ds: DataSource) : FileModelDao {
    override fun getFileModel(versionId: Int): VersionedFileModel? {
        val dbFileModel = getDbFileModel(versionId) ?: return null
        val dbColumns = getDbFileModelColumns(versionId)

        val versionInfo = adaptVersionInfo(dbFileModel)
        val fileModel = when (dbFileModel.type) {
            DbFileModel.Type.CSV -> adaptCsvFileModel(dbFileModel, dbColumns)
            DbFileModel.Type.FIXED_WIDTH -> adaptFixedWidthFileModel(dbFileModel, dbColumns)
        }

        return VersionedFileModel(
                fileId = versionInfo.fileId,
                versionId = versionInfo.versionId,
                active = versionInfo.active,
                activeReconDate = versionInfo.activeReconDate,
                tsCreated = versionInfo.tsCreated,
                createdBy = versionInfo.createdBy,
                fileModel = fileModel
        )
    }

    override fun getActiveFileModel(fileId: Int, date: LocalDate): VersionedFileModel? {
        val versionId = getActiveVersionId(fileId, date) ?: return null
        return getFileModel(versionId)
    }

    private fun getActiveVersionId(fileId: Int, date: LocalDate): Int? {
        val sql = """
            SELECT Version
            FROM prometheusrawdata.FileModelsHistory fm
            WHERE fm.Useable = 1
            AND fm.Active = 1
            AND fm.FileId = ?
            AND fm.ActiveReconDate <= ?
            ORDER BY
                fm.ActiveReconDate DESC,
                fm.Created DESC
        """

        ds.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.setInt(1, fileId)
                stmt.setGlobalDate(2, date)
                stmt.executeQuery().use { rs ->
                    if (!rs.next())
                        return null
                    return rs.getInt(1)
                }
            }
        }
    }

    data class DbFileModelColumn(
            val name: String,
            val identifier: Boolean,
            val nullable: Boolean,
            val dataType: DataType,
            val position: Int,
            val length: Int,
            val format: String?
    ) {
        enum class DataType { STRING, INTEGER, DOUBLE, DATE }
    }
    private fun getDbFileModelColumns(versionId: Int): List<DbFileModelColumn> {
        val sql = """
            SELECT *
            FROM prometheusrawdata.FileModelColumns fmc
            WHERE fmc.FileModelVersion = ?
            ORDER BY fmc.Position
            """

        ds.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.setInt(1, versionId)
                stmt.executeQuery().use { rs ->
                    val ret = mutableListOf<DbFileModelColumn>()
                    while (rs.next()) {
                        ret.add(DbFileModelColumn(
                                name = rs.getString("Name"),
                                identifier = rs.getBoolean("Identifier"),
                                nullable = rs.getBoolean("Nullable"),
                                dataType = DbFileModelColumn.DataType.valueOf(rs.getString("DataType")),
                                position = rs.getInt("Position"),
                                length = rs.getInt("Length"),
                                format = rs.getString("Format")
                        ))
                    }
                    return ret
                }
            }
        }
    }

    data class DbFileModel(
            val fileId: Int,
            val activeReconDate: LocalDate,
            val versionId: Int,
            val description: String,
            val type: Type,
            val delimiter: String?,
            val numHeaderLines: Int,
            val numFooterLines: Int,
            val notes: String,
            val active: Boolean,
            val tsCreated: Instant,
            // val primaryKeyColumns: List<String>,
            val useable: Boolean,
            val staticFile: Boolean
    ) {
        enum class Type { CSV, FIXED_WIDTH }
    }

    private fun getDbFileModel(versionId: Int): DbFileModel? {
        val sql = """
            SELECT *
            FROM prometheusrawdata.FileModelsHistory fm
            WHERE fm.Version = ?
            """

        ds.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.setInt(1, versionId)
                stmt.executeQuery().use { rs ->
                    if (!rs.next())
                        return null

                    return DbFileModel(
                            fileId = rs.getInt("FileID"),
                            activeReconDate = rs.getGlobalDate("ActiveReconDate")!!,
                            versionId = rs.getInt("Version"),
                            description = rs.getString("Description") ?: "",
                            type = DbFileModel.Type.valueOf(rs.getString("Type")),
                            delimiter = rs.getString("Delimiter"),
                            numHeaderLines = rs.getInt("HeaderLines"),
                            numFooterLines = rs.getInt("FooterLines"),
                            notes = rs.getString("Notes") ?: "",
                            active = rs.getBoolean("Active"),
                            tsCreated = rs.getTimestamp("Created").toInstant(),
                            useable = rs.getBoolean("Useable"),
                            staticFile = rs.getBoolean("StaticFile")
                    )
                }
            }
        }
    }

    data class DbVersionInfo(
            val fileId: Int,
            val versionId: Long,
            val active: Boolean,
            val activeReconDate: LocalDate,
            val tsCreated: Instant,
            val createdBy: String)

    companion object {
        fun adaptVersionInfo(dbFm: DbFileModel): DbVersionInfo =
                DbVersionInfo(
                    fileId = dbFm.fileId,
                    versionId = dbFm.versionId.toLong(),
                    tsCreated = dbFm.tsCreated,
                    activeReconDate = dbFm.activeReconDate,
                    active = dbFm.active,
                    createdBy = "unknown")

        fun adaptCsvFileModel(dbFm: DbFileModel, dbColumns: List<DbFileModelColumn>): CsvFileModel {
            require(dbFm.type == DbFileModel.Type.CSV)
            require(dbFm.delimiter != null) { "null delimiter in csv file model  id:${dbFm.fileId}  v:${dbFm.versionId}" }

            return CsvFileModel(
                    numHeaderLines = dbFm.numHeaderLines,
                    numFooterLines = dbFm.numFooterLines,
                    delimiter = dbFm.delimiter!!,
                    columns = dbColumns.map { adaptCsvColumn(it) }
            )
        }

        private fun adaptCsvColumn(dbCol: DbFileModelColumn): CsvFileModel.Column {
            return CsvFileModel.Column(
                    name = dbCol.name,
                    type = adaptColumnType(dbCol),
                    isIdentifier = dbCol.identifier
            )
        }

        fun adaptFixedWidthFileModel(dbFm: DbFileModel, dbColumns: List<DbFileModelColumn>): FixedWidthFileModel {
            require(dbFm.type == DbFileModel.Type.FIXED_WIDTH)

            return FixedWidthFileModel(
                    numHeaderLines = dbFm.numHeaderLines,
                    numFooterLines = dbFm.numFooterLines,
                    columns = dbColumns.map { adaptFixedWidthColumn(it) }
            )
        }

        private fun adaptFixedWidthColumn(dbCol: DbFileModelColumn): FixedWidthFileModel.Column {
            return FixedWidthFileModel.Column(
                    name = dbCol.name,
                    offset = dbCol.position,
                    width = dbCol.length,
                    type = adaptColumnType(dbCol),
                    isIdentifier = dbCol.identifier
            )
        }

        private val DATE_FORMAT_REGEX = """date:"([^"]+)"""".toRegex()
        private const val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
        private fun adaptColumnType(dbCol: DbFileModelColumn): ColumnType {
            val format = dbCol.format ?: ""

            val nullValues = mutableListOf<String>()
            if (dbCol.nullable)
                nullValues.add("")

            return when (dbCol.dataType) {
                DbFileModelColumn.DataType.STRING ->
                    ColumnTypeString(nullValues)
                DbFileModelColumn.DataType.INTEGER ->
                    ColumnTypeInt(nullValues = nullValues)
                DbFileModelColumn.DataType.DOUBLE ->
                    ColumnTypeFloat(nullValues = nullValues)
                DbFileModelColumn.DataType.DATE -> {
                    val dateFmt = DATE_FORMAT_REGEX.find(format)?.groupValues?.get(1) ?: DEFAULT_DATE_FORMAT
                    ColumnTypeDate(format = dateFmt, nullValues = nullValues)
                }
            }
        }
    }
}

private val GLOBAL_DATE_ZERO = LocalDate.of(1999, 12, 31)
private val GLOBAL_DATE_ZERO_EPOCH_DAY = GLOBAL_DATE_ZERO.toEpochDay()

private fun ResultSet.getGlobalDate(columnLabel: String): LocalDate? {
    val dayNum = getInt(columnLabel)
    if (dayNum == 0 && wasNull())
        return null

    return LocalDate.ofEpochDay(GLOBAL_DATE_ZERO_EPOCH_DAY + dayNum)
}

private fun PreparedStatement.setGlobalDate(parameterIndex: Int, date: LocalDate) {
    val epochDate = date.toEpochDay()
    val globalDayNum = (epochDate - GLOBAL_DATE_ZERO_EPOCH_DAY).toInt()
    setInt(parameterIndex, globalDayNum)
}



fun main(args: Array<String>) {
    val dao = FileModelDaoImpl(devCustDb())

    //dao.getDbFileModelColumns(6502)
    //        .forEach { println(it) }
    //dao.getDbFileModel(6502)
    //        .let { println(it) }

    //sequenceOf(6502, 1981)
    //        .map { dao.getFileModel(it) }
    //        .forEach { println(it) }

    val vfm = dao.getActiveFileModel(4508, LocalDate.of(2018, 4, 10))
    println(vfm)

    //val fm = vfm?.fileModel
    //when (fm) {
    //    is CsvFileModel -> fm.columns.forEach { println("${it.name} - ${it.type.isNullable} - ${it.type.nullValues}") }
    //    is FixedWidthFileModel -> fm.columns.forEach { println("${it.name} - ${it.type.isNullable} - ${it.type.nullValues}") }
    //}
}

fun devCustDb(): DataSource {
    val config = HikariConfig()
    config.jdbcUrl = "jdbc:sqlserver://dev-cust-db;database=CustodyData"
    config.driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    config.username = "prometheus"
    config.password = "prometheus"

    return HikariDataSource(config)
}
