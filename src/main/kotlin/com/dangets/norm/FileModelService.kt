package com.dangets.norm

import com.google.common.eventbus.EventBus
import java.time.Instant
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

sealed class FileModelCommand {
    abstract val id: UUID
    abstract val username: String
    abstract val note: String
}
data class CreateFileModel(override val username: String,
                           override val note: String,
                           val fileId: Int,
                           val activeReconDate: LocalDate,
                           val active: Boolean,
                           val fileModel: FileModel,
                           override val id: UUID = UUID.randomUUID()) : FileModelCommand()

data class UpdateFileModel(override val username: String,
                           override val note: String,
                           val versionId: VersionId,
                           val activeReconDate: LocalDate?,
                           val active: Boolean?,
                           override val id: UUID = UUID.randomUUID()) : FileModelCommand()


sealed class FileModelEvent
data class FileModelCreated(val value: VersionedFileModel) : FileModelEvent()
data class FileModelUpdated(val value: VersionedFileModel) : FileModelEvent()
data class FileModelCommandRejected(val cmd: FileModelCommand, val reason: String) : FileModelEvent()



interface FileModelService {
    // Commands ---------------
    /**
     * By default deactivate all other fileModels for this same fileId for given range?
     */
    fun createFileModel(cmd: CreateFileModel): CompletableFuture<Result<VersionId, String>>

    fun updateFileModel(cmd: UpdateFileModel): CompletableFuture<Result<Boolean, String>>

    // Queries ----------------

    fun getFileModel(versionId: VersionId): CompletableFuture<VersionedFileModel?>

    fun getFileModel(fileId: Int, reconDate: LocalDate): CompletableFuture<VersionedFileModel?>

    fun getFileModelVersions(fileId: Int): CompletableFuture<List<VersionedFileModel>>
}


class FileModelServiceImpl(private val eventBus: EventBus) : FileModelService {
    private val nextVersionId = AtomicLong(0)
    private val store = ConcurrentHashMap<VersionId, VersionedFileModel>()

    override fun createFileModel(cmd: CreateFileModel): CompletableFuture<Result<VersionId, String>> {
        val id = nextVersionId.getAndIncrement()
        val vfm = VersionedFileModel(
                fileId = cmd.fileId,
                versionId = id,
                active = cmd.active,
                activeReconDate = cmd.activeReconDate,
                tsCreated = Instant.now(),
                createdBy = cmd.username,
                fileModel = cmd.fileModel)

        return CompletableFuture.supplyAsync {
            eventBus.post(FileModelCreated(vfm))
            store[id] = vfm
            Result.Ok<VersionId, String>(id)
        }
    }

    override fun updateFileModel(cmd: UpdateFileModel): CompletableFuture<Result<Boolean, String>> {
        val vfm = store[cmd.versionId]
        if (vfm == null) {
            val error = "file model version ${cmd.versionId} not found"
            eventBus.post(FileModelCommandRejected(cmd, error))
            return CompletableFuture.completedFuture(Result.Err(error))
        }

        val updated = vfm.copy(active = cmd.active ?: vfm.active, activeReconDate = cmd.activeReconDate ?: vfm.activeReconDate)
        // could check if updated == vfm

        return CompletableFuture.supplyAsync {
            eventBus.post(FileModelUpdated(updated))
            store[updated.versionId] = updated
            Result.Ok<Boolean, String>(true)
        }
    }

    override fun getFileModel(versionId: VersionId): CompletableFuture<VersionedFileModel?> {
        return CompletableFuture.completedFuture(store[versionId])
    }

    override fun getFileModel(fileId: Int, reconDate: LocalDate): CompletableFuture<VersionedFileModel?> {
        return getFileModelVersions(fileId)
                .thenApply { fms ->
                    fms.sortedByDescending { it.activeReconDate }
                            .first { it.activeReconDate <= reconDate }
                }
    }

    override fun getFileModelVersions(fileId: Int): CompletableFuture<List<VersionedFileModel>> {
        return CompletableFuture.supplyAsync {
            store.values.filter { it.fileId == fileId }
        }
    }
}
