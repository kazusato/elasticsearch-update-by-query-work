package kazusato.eswork.updatebyquery.command

import kazusato.eswork.updatebyquery.UpdateByQueryContext
import kazusato.eswork.updatebyquery.base.AbstractCommand
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.GetIndexRequest

class ClearCommand : AbstractCommand<UpdateByQueryContext> {

    constructor(commandArgs: Array<String>, context: UpdateByQueryContext) : super(commandArgs, context) {
    }

    override fun executeCommand() {
        if (checkIfIndexExists()) {
            deleteIndex()
        }
    }

    private fun checkIfIndexExists(): Boolean {
        val req = GetIndexRequest("storeinfo")
        val indexExists = context.clientManager.client.indices().exists(req, RequestOptions.DEFAULT)
        return indexExists
    }

    private fun deleteIndex() {
        val req = DeleteIndexRequest("storeinfo")
        val resp = context.clientManager.client.indices().delete(req, RequestOptions.DEFAULT)
        println("[DELETE INDEX] Response: isAcknowledged => ${resp.isAcknowledged}")
    }
}