package kazusato.eswork.updatebyquery.base

abstract class AbstractCommand<out T> {

    protected val commandArgs : Array<String>

    protected val context : T

    constructor(commandArgs: Array<String>, context: T) {
        this.commandArgs = commandArgs
        this.context = context
    }

    abstract fun executeCommand()
}