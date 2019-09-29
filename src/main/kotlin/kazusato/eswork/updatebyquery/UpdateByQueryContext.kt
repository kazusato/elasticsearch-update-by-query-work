package kazusato.eswork.updatebyquery

import kazusato.eswork.updatebyquery.base.CommandContext
import kazusato.eswork.updatebyquery.elasticsearch.ClientManager

data class UpdateByQueryContext(
        val clientManager : ClientManager
) : CommandContext() {
}
