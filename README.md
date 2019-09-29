# Instruction to prepare environment

## docker-compose up

To run Elasticsearch and Kibana on the local environment, execute the following commands.

```
# create data directory (required only in the first time)
$ mkdir -p esdata
# start Elasticsearch and Kibana containers
$ cd docker
$ docker-compose up -d
```

# Apply index settings and mappings

Execute the following command to create an index with specified settings and mappings.
 
```
$ curl -XPUT -H "Content-Type: application/json" -d @es-json/01-index-mapping.json http://localhost:9200/storeinfo
```

# Create initial documents

