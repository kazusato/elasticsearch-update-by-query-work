package kazusato.eswork.updatebyquery.command

import kazusato.eswork.updatebyquery.UpdateByQueryContext
import kazusato.eswork.updatebyquery.base.AbstractCommand
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions

class HealthCommand : AbstractCommand<UpdateByQueryContext> {

    constructor(commandArgs: Array<String>, context: UpdateByQueryContext) : super(commandArgs, context) {
    }

    override fun executeCommand() {
        val req = ClusterHealthRequest()
        val resp = context.clientManager.client.cluster().health(req, RequestOptions.DEFAULT)
        println(resp.clusterName)
    }

}