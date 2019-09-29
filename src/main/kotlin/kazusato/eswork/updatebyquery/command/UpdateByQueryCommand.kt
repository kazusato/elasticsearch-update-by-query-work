package kazusato.eswork.updatebyquery.command

import kazusato.eswork.updatebyquery.UpdateByQueryContext
import kazusato.eswork.updatebyquery.base.AbstractCommand
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.TermsQueryBuilder
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType

class UpdateByQueryCommand : AbstractCommand<UpdateByQueryContext> {

    constructor(commandArgs: Array<String>, context: UpdateByQueryContext) : super(commandArgs, context) {
    }

    override fun executeCommand() {
        if (commandArgs.size == 0) {
            throw IllegalArgumentException("specify target area (such as 'Tokyo' or 'Osaka')")
        }
        val firstArg = commandArgs[0]
        updateDocuments(firstArg)
    }

    private fun updateDocuments(targetArea: String) {
        val req = UpdateByQueryRequest("storeinfo")
        req.setQuery(BoolQueryBuilder()
                .must(TermsQueryBuilder("category", "bookstore"))
                .must(TermsQueryBuilder("area", targetArea)))
        req.setScript(Script(ScriptType.INLINE, "painless",
                "ctx._source.store_type=params.store_type;" +
                        "ctx._source.data_schema=params.data_schema",
                mapOf(
                        "store_type" to "bookstore_$targetArea",
                        "data_schema" to "v2"
                )
        ))
        req.setScroll(TimeValue.timeValueMinutes(1L))
        val resp = context.clientManager.client.updateByQuery(req, RequestOptions.DEFAULT)
        println("[UPDATE BY QUERY] Response: totalDocs => ${resp.total}, took => ${resp.took}, timedOut => ${resp.isTimedOut}")
    }

}