package com.dangets.norm

import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.rxkotlin.toFlowable
import io.reactivex.subjects.PublishSubject
import java.time.Instant
import java.time.LocalDate
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

sealed class CommandOrEvent<C, E> { abstract val id: UUID }
data class Command<T>(override val id: UUID, val value: T) : CommandOrEvent<T, Nothing>()
data class Event<T>(override val id: UUID, val value: T) : CommandOrEvent<Nothing, T>()

sealed class FileModelEvent {
    data class FileModelCreated(val value: VersionedFileModel) : FileModelEvent()
    data class FileModelInactivated(val versionId: VersionId) : FileModelEvent()
    data class FileModelCommandRejected(val cmd: FileModelCommand, val reason: String) : FileModelEvent()
}


sealed class FileModelCommand {
    abstract val username: String
    abstract val note: String

    data class CreateFileModel(override val username: String,
                               override val note: String,
                               val fileId: Int,
                               val activeReconDate: LocalDate,
                               val active: Boolean,
                               val fileModel: FileModel) : FileModelCommand()

    /**
     * Create a new file model version from existing file model version.
     * Original file model version will be inactivated.
     */
    data class UpdateFileModel(override val username: String,
                               override val note: String,
                               val versionId: VersionId,
                               val activeReconDate: LocalDate,
                               val active: Boolean,
                               val fileModel: FileModel) : FileModelCommand()

    data class SetActiveReconDate(override val username: String,
                                  override val note: String,
                                  val versionId: VersionId,
                                  val activeReconDate: LocalDate) : FileModelCommand()

    data class InactivateFileModel(override val username: String,
                                   override val note: String,
                                   val versionId: VersionId) : FileModelCommand()
}

interface FileModelCommandApi {
    fun post(cmd: FileModelCommand): Single<UUID>

    //fun postSync(cmd: FileModelCommand)
    //fun subscribe(): Flowable<CommandOrEvent<FileModelCommand, FileModelEvent>>
}

class FileModelCommandApiImpl(private val cmdBus: PublishSubject<Command<FileModelCommand>>): FileModelCommandApi {
    override fun post(cmd: FileModelCommand): Single<UUID> {
        val id = UUID.randomUUID()
        val c = Command(id, cmd)
        println("cmdApi: posting to cmdBus $c")
        cmdBus.onNext(c)
        return Single.just(id)
    }
}

class FileModelCommandProcessor(cmdBus: PublishSubject<Command<FileModelCommand>>,
                                private val eventBus: PublishSubject<Event<FileModelEvent>>) {
    private val nextVersionId = AtomicLong(0)
    private val store = ConcurrentHashMap<VersionId, VersionedFileModel>()

    init {
        cmdBus.subscribe(this::onCmd)
    }

    private fun postEvent(event: FileModelEvent) {
        val uuid = UUID.randomUUID()
        println("processor: posting to eventBus $event")
        eventBus.onNext(Event(uuid, event))
    }

    fun onCmd(cmd: Command<FileModelCommand>) {
        println("processor: received $cmd")
        when (cmd.value) {
            is FileModelCommand.CreateFileModel -> onCreateFileModel(cmd.value)
        }
    }

    private fun onCreateFileModel(cmd: FileModelCommand.CreateFileModel) {
        val id = nextVersionId.getAndIncrement()
        val vfm = VersionedFileModel(
                fileId = cmd.fileId,
                versionId = id,
                active = cmd.active,
                activeReconDate = cmd.activeReconDate,
                tsCreated = Instant.now(),
                createdBy = cmd.username,
                fileModel = cmd.fileModel)

        store[id] = vfm
        postEvent(FileModelEvent.FileModelCreated(vfm))
    }
}

class FileModelView(eventBus: PublishSubject<Event<FileModelEvent>>) {
    private val store = ConcurrentHashMap<VersionId, VersionedFileModel>()

    init {
        eventBus.subscribe(this::onEvent)
    }

    private fun onEvent(event: Event<FileModelEvent>) {
        println("view: received $event")
        when (event.value) {
            is FileModelEvent.FileModelCreated -> onFileModelCreated(event.value)
        }
    }

    private fun onFileModelCreated(event: FileModelEvent.FileModelCreated) {
        val vfm = event.value
        store[vfm.versionId] = vfm
    }

    fun getFileModels(): Flowable<VersionedFileModel> = store.values.toFlowable()
}


fun main(args: Array<String>) {
    val cmdBus = PublishSubject.create<Command<FileModelCommand>>()
    val eventBus = PublishSubject.create<Event<FileModelEvent>>()

    val fmView = FileModelView(eventBus)
    val fmView2 = FileModelView(eventBus)
    val fmView3 = FileModelView(eventBus)
    val cmdProc = FileModelCommandProcessor(cmdBus, eventBus)
    val cmdApi = FileModelCommandApiImpl(cmdBus)

    println("1: " + fmView.getFileModels().toList().blockingGet())
    cmdApi.post(FileModelCommand.CreateFileModel(
            username = "dannygeorge",
            note = "first commit",
            fileId = 231,
            activeReconDate = LocalDate.parse("2018-01-31"),
            active = true,
            fileModel = CsvFileModel(
                    numHeaderLines = 1,
                    numFooterLines = 0,
                    delimiter = ",",
                    columns = listOf(
                            CsvFileModel.Column(name = "accountId", type = ColumnTypeInt(), isIdentifier = false)
                    )
            )
    ))
    println("2: " + fmView.getFileModels().toList().blockingGet())
}
