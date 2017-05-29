package com.dsetton

import com.arangodb.ArangoCursor
import com.arangodb.ArangoDB
import com.arangodb.ArangoDBAsync
import com.arangodb.ArangoDBException
import com.arangodb.entity.DocumentCreateEntity
import com.arangodb.util.MapBuilder
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj

object ArangoHelper {

    val arangoDBSync: ArangoDB = ArangoDB.Builder().build()
    val arangoDBAsync: ArangoDBAsync = ArangoDBAsync.Builder().build()

    fun prepareDBSync(vertx: Vertx, dbName:String, collectionName:String): Future<Void> {
        val fut = Future.future<Void>()

        vertx.executeBlocking<Void>({
//            try {
//                arangoDBSync.db(dbName).drop()
//            } catch (e: Exception) {
//
//            }
            try {
                arangoDBSync.createDatabase(dbName)
            } catch (e: ArangoDBException) {
                println("Failed to create database: $dbName")
            }

            try {
                arangoDBSync.db(dbName).createCollection(collectionName)
            } catch (e: ArangoDBException) {
                System.err.println("Failed to create collection: $collectionName")
            }
            it.complete()
        }, {
            fut.complete()
        })


        return fut
    }

    fun prepareDBAsync(dbName:String, collectionName:String): Future<Void> {

        val fut = Future.future<Void>()

        try {
//            arangoDBAsync.db(dbName).drop()
            arangoDBAsync.createDatabase(dbName)
                    .thenAccept ({
                        arangoDBAsync.db(dbName).createCollection(collectionName)
                                .thenAccept({
                                    fut.complete()
                                })
                    })
        } catch (e: ArangoDBException) {
            println("Failed to create database: $dbName")
        }

        return fut
    }


    fun insertJsonSync(vertx: Vertx, dbName:String, collectionName:String, data: JsonArray, index:Int = 0, client: ArangoDB, future: Future<Any>){

        vertx.executeBlocking<Any>({
            val obj: JsonObject = data.getJsonObject(index)
            val doc: DocumentCreateEntity<String> = client.db(dbName).collection(collectionName).insertDocument(obj.toString())
            it.complete(doc)
        }, {
            if (!it.failed()) {
                if (index == data.size() - 1){
                    future.complete("Arango INSERT Sync to $dbName ")
                } else {
                    insertJsonSync(vertx, dbName, collectionName, data, index.inc(), client, future)
                }
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })
    }

    fun insertJsonAsync(dbName:String, collectionName:String, data: JsonArray, index:Int = 0, client: ArangoDBAsync, future: Future<Any>){

        val testObj: JsonObject = data.getJsonObject(index)
        client.db(dbName).collection(collectionName).insertDocument(testObj.toString())
                .thenAccept({
                    if (index == data.size() - 1){
                        future.complete("Arango INSERT Async to $dbName ")
                    } else {
                        insertJsonAsync(dbName, collectionName, data, index.inc(), client, future)
                    }
                })
    }

    fun fullScanJsonSync(vertx:Vertx, dbName:String, collectionName:String, client: ArangoDB, future: Future<Any>){

        vertx.executeBlocking<ArangoCursor<JsonObject>>({
            val query = "FOR o IN $collectionName RETURN o"
            val cursor = client.db(dbName).query(query, null, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
                vertx.executeBlocking<List<JsonObject>>({ fut ->
                    val listRemaining = it.result().asListRemaining()
                    fut.complete(listRemaining)
                }, {
                    future.complete("Arango SYNC Step1: read ${it.result().size} objects")
                })
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     *  Step1 - Leitura de todos os registros da tabela
     **/
    fun fullScanAsync(vertx: Vertx, dbName: String, collectionName: String, client: ArangoDBAsync, future: Future<Any>) {

        val query = "FOR o IN $collectionName RETURN o"

//        val bindVars = MapBuilder().put("collectionName", collectionName).get()
        val completableFuture = client.db(dbName).query(query, null, null, String::class.javaObjectType)

        completableFuture.whenComplete({ cursor, ex ->
            if (ex != null) {
                ex.printStackTrace()
            } else {
                vertx.executeBlocking<List<String>>({ fut ->
                    val listRemaining = cursor.asListRemaining()
                    fut.complete(listRemaining)
                }, {
                    future.complete("Arango ASYNC Step1: read ${it.result().size} objects")
//                    future.complete("Arango ASYNC Step1 Full Scan cursor in ${cursor.stats.scannedFull}")
                })
            }
        })
        completableFuture.get()

    }

    /**
     * Step 2 - leitura com apresentação de todas as colunas para o CPF = 211.211.224-21
     * */
    fun step2Sync(vertx: Vertx, client: ArangoDB, dbName: String, collectionName: String, field: String, value: String, future: Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({
            val query = "FOR o IN @@collection " +
                    "   FILTER o.@field == @value" +
                    "   RETURN o"

            val bindVars = MapBuilder()
                    .put("@collection", collectionName)
                    .put("field", field)
                    .put("value", value)
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
                val listRemaining = it.result().asListRemaining()
                future.complete("Arango SYNC Step2: read ${listRemaining.size} objects")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     * 3 - leitura com apresentação das colunas A, B e E para os
     * CPFs IN (122.432.104-14,324.431.124-02,443.112.312-00,144.323.134-11,314.120.111-20)
     */
    fun step3Sync(vertx: Vertx, client: ArangoDB, dbName: String, collection:String, queryField:String, values:JsonArray, displayFields:JsonArray, future: Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({

            val projectionObj = json {
                obj{
                    displayFields.forEach{
                        put(it as String, "o[$it]")
                    }
                }
            }

            val query = "FOR o IN @@collection " +
                    "   FILTER o.@field IN @value " +
                    "   RETURN ".plus(projectionObj.toString())

            val bindVars = MapBuilder()
                    .put("@collection", collection)
                    .put("field", queryField)
                    .put("value", values.toList())
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
                val listRemaining = it.result().asListRemaining()
                future.complete("Arango SYNC Step3: read ${listRemaining.size} objects")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     * Step 4 - leitura com apresentação de todas as colunas para os contratos criados em 2015 ou seja Data do Contrato entre 01/01/2015 e 31/12/2015
     * */
    fun step4Sync(vertx: Vertx, client: ArangoDB, dbName: String, collection:String, field:String, upperBound:Any, lowerBound:Any, future:Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({

            val query = "FOR o IN @@collection " +
                    "   FILTER o[@field] >= @lowerBound && o[@field] <= @upperBound" +
                    "   RETURN o "

            val bindVars = MapBuilder()
                    .put("@collection", collection)
                    .put("field", field)
                    .put("lowerBound", lowerBound)
                    .put("upperBound", upperBound)
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
                val listRemaining = it.result().asListRemaining()
                future.complete("Arango SYNC Step4: read ${listRemaining.size} objects")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     * Step 5 - leitura com apresentação TOTAL ( Principal ) dos contratos criados em 2016 ou seja Data do Contrato entre 01/01/2016 e 31/12/2016.
     * */
    fun step5Sync(vertx: Vertx, client: ArangoDB, dbName: String, collection:String, queryField:String, sumField:String, upperBound:Any, lowerBound:Any, future:Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({

            val query =
                    "RETURN {'total': SUM(" +
                    "   FOR o IN @@collection " +
                    "   FILTER o[@queryField] >= @lowerBound && o[@queryField] <= @upperBound" +
                    "   RETURN o[@sumField]" +
                    ")" +
                    "}"

            val bindVars = MapBuilder()
                    .put("@collection", collection)
                    .put("queryField", queryField)
                    .put("sumField", sumField)
                    .put("lowerBound", lowerBound)
                    .put("upperBound", upperBound)
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
//                val listRemaining:ArangoCursor = it.result().asListRemaining()
//                future.complete("Arango SYNC SUM($sumField): value ${listRemaining[0].getDouble("total")}")
//                future.complete("Arango SYNC SUM($sumField): value ${it.result().next().getDouble("total")}")
                future.complete("Arango SYNC step5 done")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     * 6 - leitura com apresentação TOTAL ( Principal ) e TOTAL ( Interest ) agrupados por ANO.
     * */
    fun step6Sync(vertx: Vertx, client: ArangoDB, dbName: String, collection:String, groupField:String, sumField1:String, sumField2:String, future:Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({

            val query = "FOR o IN @@collection " +
                    "   COLLECT groupVar = o[@groupField] into groups" +
                    "   return {" +
                    "       @groupField : groupVar," +
                    "       @sumField1 : SUM(groups[*].o[@sumField1])," +
                    "       @sumField2 : SUM(groups[*].o[@sumField2])" +
                    "   }"

            val bindVars = MapBuilder()
                    .put("@collection", collection)
                    .put("groupField", groupField)
                    .put("sumField1", sumField1)
                    .put("sumField2", sumField2)
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
//                val listRemaining:ArangoCursor = it.result().asListRemaining()
//                future.complete("Arango SYNC SUM($sumField): value ${it.result().next().getDouble("total")}")
//                future.complete("Arango SYNC step6 done: $year1 (total_$sumField1 = $total1y1, total_$sumField2 = $total2y1) ; $year2 (total_$sumField1 = $total1y2, total_$sumField2 = $total2y2)" )
                future.complete("Arango SYNC step6 done")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }

    /**
     * Step 7 - leitura com apresentação de todas colunas para os contratos de 2016 com principal maior que 15000.
     * */
    fun step7Sync(vertx: Vertx, client: ArangoDB, dbName: String, collection:String, queryField:String, valueQueryField:String, field2:String, lowerBoundField2:Any, future:Future<Any>) {

        vertx.executeBlocking<ArangoCursor<JsonObject>>({

            val query = "FOR o IN @@collection " +
                    "   FILTER o[@queryField] == @valueQueryField && o[@field2] >= @lowerBoundField2 " +
                    "   return o "

            val bindVars = MapBuilder()
                    .put("@collection", collection)
                    .put("queryField", queryField)
                    .put("valueQueryField", valueQueryField)
                    .put("field2", field2)
                    .put("lowerBoundField2", lowerBoundField2)
                    .get()
            val cursor = client.db(dbName).query(query, bindVars, null, JsonObject::class.javaObjectType)
            it.complete(cursor)
        }, {
            if (!it.failed()) {
//                val listRemaining:ArangoCursor = it.result().asListRemaining()
                future.complete("Arango SYNC step7 done")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })

    }
}