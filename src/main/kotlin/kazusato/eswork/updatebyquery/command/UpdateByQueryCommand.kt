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
        if (commandArgs.size < 2) {
            throw IllegalArgumentException("specify a schema version (such as 'v2' or 'v3') and a target area (such as 'Tokyo' or 'Osaka')")
        }
        val schemaVersion = commandArgs[0]
        val targetArea = commandArgs[1]
        when(schemaVersion) {
            "v2" -> updateDocumentsV2(targetArea)
            "v3" -> updateDocumentsV3(targetArea)
            "v4" -> updateDocumentsV4(targetArea)
            "v5" -> updateDocumentsV5(targetArea)
            "v6" -> updateDocumentsV6(targetArea)
            else -> throw java.lang.IllegalArgumentException("Unsupported version $schemaVersion")
        }
    }

    private fun updateDocumentsV2(targetArea: String) {
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

    private fun updateDocumentsV3(targetArea: String) {
        val req = UpdateByQueryRequest("storeinfo")
        req.setQuery(BoolQueryBuilder()
                .must(TermsQueryBuilder("category", "bookstore"))
                .must(TermsQueryBuilder("area", targetArea)))
        val shortArea = when(targetArea) {
            "Tokyo" -> "TYO"
            "Osaka" -> "OSA"
            else -> "UNK"
        }
        req.setScript(Script(ScriptType.INLINE, "painless",
                "ctx._source.store_type=params.store_type;" +
                        "ctx._source.data_schema=params.data_schema",
                mapOf(
                        "store_type" to "BK$shortArea",
                        "data_schema" to "v3"
                )
        ))
        req.setScroll(TimeValue.timeValueMinutes(1L))
        val resp = context.clientManager.client.updateByQuery(req, RequestOptions.DEFAULT)
        println("[UPDATE BY QUERY] Response: totalDocs => ${resp.total}, took => ${resp.took}, timedOut => ${resp.isTimedOut}")
    }

    private fun updateDocumentsV4(targetArea: String) {
        val req = UpdateByQueryRequest("storeinfo")
        req.setQuery(BoolQueryBuilder()
                .must(TermsQueryBuilder("category", "bookstore"))
                .must(TermsQueryBuilder("area", targetArea)))
        val shortArea = when(targetArea) {
            "Tokyo" -> "TYO"
            "Osaka" -> "OSA"
            else -> "UNK"
        }
        req.setScript(Script(ScriptType.INLINE, "painless",
                "ctx._source.tags=params.tags;" +
                        "ctx._source.data_schema=params.data_schema",
                mapOf(
                        "tags" to listOf(
                                "bookstore",
                                targetArea,
                                "BK$shortArea"
                        ),
                        "data_schema" to "v4"
                )
        ))
        req.setScroll(TimeValue.timeValueMinutes(1L))
        val resp = context.clientManager.client.updateByQuery(req, RequestOptions.DEFAULT)
        println("[UPDATE BY QUERY] Response: totalDocs => ${resp.total}, took => ${resp.took}, timedOut => ${resp.isTimedOut}")
    }

    private fun updateDocumentsV5(targetArea: String) {
        val req = UpdateByQueryRequest("storeinfo")
        req.setQuery(BoolQueryBuilder()
                .must(TermsQueryBuilder("category", "bookstore"))
                .must(TermsQueryBuilder("area", targetArea)))
        // When ctx._source.details does not exist yet, setting a value to ctx._source.details.store_type directly
        // is rejected because of a null pointer exception in Elasticsearch.
        req.setScript(Script(ScriptType.INLINE, "painless",
                "ctx._source.details=params.details;" +
                        "ctx._source.data_schema=params.data_schema",
                mapOf(
                        "details" to mapOf("store_type" to "bookstore_$targetArea"),
                        "data_schema" to "v5"
                )
        ))
        req.setScroll(TimeValue.timeValueMinutes(1L))
        val resp = context.clientManager.client.updateByQuery(req, RequestOptions.DEFAULT)
        println("[UPDATE BY QUERY] Response: totalDocs => ${resp.total}, took => ${resp.took}, timedOut => ${resp.isTimedOut}")
    }

    private fun updateDocumentsV6(targetArea: String) {
        val req = UpdateByQueryRequest("storeinfo")
        req.setQuery(BoolQueryBuilder()
                .must(TermsQueryBuilder("category", "bookstore"))
                .must(TermsQueryBuilder("area", targetArea)))
        val shortArea = when(targetArea) {
            "Tokyo" -> "TYO"
            "Osaka" -> "OSA"
            else -> "UNK"
        }
        // Once a parent (ctx._source.details) is created, not only directly updating to
        // a child (ctx._source.details.store_type) but also adding an unset child
        // (ctx._source.details.area) are accepted.
        req.setScript(Script(ScriptType.INLINE, "painless",
                "ctx._source.details.store_type=params.store_type;" +
                        "ctx._source.details.area=params.area;" +
                        "ctx._source.data_schema=params.data_schema",
                mapOf(
                        "store_type" to "BK$shortArea",
                        "area" to targetArea,
                        "data_schema" to "v6"
                )
        ))
        req.setScroll(TimeValue.timeValueMinutes(1L))
        // Specifying max batch size: batch size must be less than or equal to 10000.
        req.setBatchSize(10_000)
        val resp = context.clientManager.client.updateByQuery(req, RequestOptions.DEFAULT)
        println("[UPDATE BY QUERY] Response: totalDocs => ${resp.total}, took => ${resp.took}, timedOut => ${resp.isTimedOut}")
    }

}