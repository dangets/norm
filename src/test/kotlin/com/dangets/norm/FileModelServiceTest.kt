package com.dangets.norm

import com.google.common.eventbus.EventBus
import org.junit.jupiter.api.Test
import java.time.LocalDate

internal class FileModelServiceTest {
    private val eventBus = EventBus()
    private val service = FileModelServiceImpl(eventBus)

    @Test
    fun `test get all FileModels given fileId`() {
        val cmd = CreateFileModel(username = "dg",
                note = "test",
                fileId = 123,
                activeReconDate = LocalDate.parse("2018-01-01"),
                active = true,
                fileModel = CsvFileModel(0, 0, ",",
                        columns = listOf(CsvFileModel.Column("accountId", type = ColumnTypeInt(), isIdentifier = true))
                ))

        val vId = service.createFileModel(cmd).join().unwrap()
        val vfm = service.getFileModel(vId).join()!!
        println(vfm)
    }
}