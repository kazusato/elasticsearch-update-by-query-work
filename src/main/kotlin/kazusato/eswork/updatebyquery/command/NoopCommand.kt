package kazusato.eswork.updatebyquery.command

import kazusato.eswork.updatebyquery.UpdateByQueryContext
import kazusato.eswork.updatebyquery.base.AbstractCommand

class NoopCommand : AbstractCommand<UpdateByQueryContext> {

    constructor(commandArgs: Array<String>, context: UpdateByQueryContext) : super(commandArgs, context) {
    }

    override fun executeCommand() {
    }

}