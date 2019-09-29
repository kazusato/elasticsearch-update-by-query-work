package kazusato.eswork.updatebyquery

import kazusato.eswork.updatebyquery.base.AbstractCommand
import kazusato.eswork.updatebyquery.command.*
import kazusato.eswork.updatebyquery.elasticsearch.ClientManager

fun main(args: Array<String>) {
    if (args.size < 1) {
        System.err.println("usage: java UpdateByQuery command")
        System.exit(1)
    }

    UpdateByQueryWork(args).use { ubq ->
        ubq.execute()
    }
}

class UpdateByQueryWork : AutoCloseable {

    private val args: Array<String>

    private val context: UpdateByQueryContext

    constructor(args: Array<String>) {
        this.args = args
        val clientManager = ClientManager()
        this.context = UpdateByQueryContext(clientManager)
    }

    fun execute() {
        val separatedCommand = separateArgs()
        val command = dispatchCommand(separatedCommand.first, separatedCommand.second)
        command.executeCommand()
    }

    override fun close() {
        context.clientManager.close()
    }

    internal fun separateArgs(): Pair<String, Array<String>> {
        val commandArgs = if (args.size >= 1) {
            args.sliceArray(1..args.size-1)
        } else {
            arrayOf()
        }

        return Pair(args[0], commandArgs)
    }

    internal fun dispatchCommand(command: String, commandArgs: Array<String>): AbstractCommand<UpdateByQueryContext> {
        when (command) {
            "noop" -> return NoopCommand(commandArgs, context)
            "init" -> return InitCommand(commandArgs, context)
            "updatebyquery" -> return UpdateByQueryCommand(commandArgs, context)
            "health" -> return HealthCommand(commandArgs, context)
            "clear" -> return ClearCommand(commandArgs, context)
            else -> throw IllegalArgumentException("No such command: $command")
        }
    }

}
