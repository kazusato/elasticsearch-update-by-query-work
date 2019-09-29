package kazusato.eswork.updatebyquery.command

import kazusato.eswork.updatebyquery.UpdateByQueryContext
import kazusato.eswork.updatebyquery.base.AbstractCommand
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.settings.Settings
import java.time.Duration
import java.time.LocalDateTime
import kotlin.math.floor

class InitCommand : AbstractCommand<UpdateByQueryContext> {

    constructor(commandArgs: Array<String>, context : UpdateByQueryContext) : super(commandArgs, context) {
    }

    override fun executeCommand() {
        if (commandArgs.size == 0) {
            throw IllegalArgumentException("specify 'default' or number of documents")
        }
        val firstArg = commandArgs[0]
        if (firstArg == "default") {
            if (checkIfIndexExists()) {
                deleteIndex()
            }
            createIndex()

            generateDefaultDocuments()
            return
        }
        if (firstArg.matches(Regex("\\d+"))) {
            if (checkIfIndexExists()) {
                deleteIndex()
            }
            createIndex()

            val count = firstArg.toLong()
            generateDocumentsWithCount(count)
            return
        }
    }

    private fun generateDefaultDocuments() {
        val bulkReq = BulkRequest()

        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "東京書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "有楽町書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "新橋書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "浜松町書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "田町書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "品川書店",
                        "category" to "bookstore",
                        "area" to "Tokyo"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "大阪書店",
                        "category" to "bookstore",
                        "area" to "Osaka"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "福島書店",
                        "category" to "bookstore",
                        "area" to "Osaka"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "野田書店",
                        "category" to "bookstore",
                        "area" to "Osaka"
                )
        ))
        bulkReq.add(IndexRequest("storeinfo").source(
                mapOf<String, Any>(
                        "name" to "西九条書店",
                        "category" to "bookstore",
                        "area" to "Osaka"
                )
        ))

        val resp = context.clientManager.client.bulk(bulkReq, RequestOptions.DEFAULT)
        println("[CREATE DOCUMENT] Response: hasFailures => ${resp.hasFailures()}")
    }

    private fun generateDocumentsWithCount(count: Long) {
        val startTime = LocalDateTime.now()
        val digit = floor(Math.log10(count.toDouble())).toInt() + 1
        val storeFormat = "書店%0${digit}d"

        val indexRequestList = mutableListOf<IndexRequest>()
        for (i in 1..count) {
            val storeName = storeFormat.format(i)
            val area = if (i % 2L == 1L) {
                "Tokyo"
            } else {
                "Osaka"
            }

            val indexRequest = IndexRequest("storeinfo").source(
                    mapOf(
                            "name" to storeName,
                            "category" to "bookstore",
                            "area" to area
                    )
            )
            indexRequestList.add(indexRequest)

            if (i % 100_000L == 0L) {
                processBulkRequest(indexRequestList, i)
                indexRequestList.clear()
            }
        }
        if (!indexRequestList.isEmpty()) {
            processBulkRequest(indexRequestList, count)
            indexRequestList.clear()
        }

        val endTime = LocalDateTime.now()
        println("[CREATE DOCUMENT] END: duration => ${Duration.between(startTime, endTime)}")
    }

    private fun processBulkRequest(indexRequestList: List<IndexRequest>, cumulativeCount: Long) {
        val startTime = LocalDateTime.now()
        val bulkReq = BulkRequest()

        indexRequestList.forEach { req ->
            bulkReq.add(req)
        }

        val resp = context.clientManager.client.bulk(bulkReq, RequestOptions.DEFAULT)
        val endTime = LocalDateTime.now()
        println("[CREATE DOCUMENT] Bulk[$cumulativeCount] Response: hasFailures => ${resp.hasFailures()}, duration => ${Duration.between(startTime, endTime)}")

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

    private fun createIndex() {
        val req = CreateIndexRequest("storeinfo")
        req.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
        )
        req.mapping(mapOf(
                "properties" to mapOf(
                        "name" to mapOf("type" to "keyword"),
                        "category" to mapOf("type" to "keyword"),
                        "area" to mapOf("type" to "keyword"),
                        "store_type" to mapOf("type" to "keyword"),
                        "data_schema" to mapOf("type" to "keyword"),
                        "tags" to mapOf("type" to "keyword"),
                        "details" to mapOf(
                                "type" to "nested",
                                "properties" to mapOf(
                                        "store_type" to mapOf("type" to "keyword"),
                                        "area" to mapOf("type" to "keyword")
                                )
                        )
                ),
                "dynamic" to "strict"
        ))
        val resp = context.clientManager.client.indices().create(req, RequestOptions.DEFAULT)
        println("[CREATE INDEX] Response: isAcknowledged => ${resp.isAcknowledged}")
    }

}