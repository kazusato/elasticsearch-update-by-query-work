package kazusato.eswork.updatebyquery.elasticsearch

import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestHighLevelClient

class ClientManager : AutoCloseable {

    val client : RestHighLevelClient

    constructor() {
        client = RestHighLevelClient(RestClient.builder(
                HttpHost("localhost", 9200, "http")
        )
                .setRequestConfigCallback(RestClientBuilder.RequestConfigCallback { builder ->
                    builder.setConnectTimeout(30_000)
                            .setSocketTimeout(60_000)
                })
        )
    }

    override fun close() {
        client.close()
    }


}