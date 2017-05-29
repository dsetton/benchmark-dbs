package com.dsetton

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.mongo.FindOptions

/**
 * Created by daniel on 17/05/17.
 */
object MongoHelper {

    fun getClient(vertx: Vertx, dbName:String): MongoClient {
        val config = json{
            obj(
                    "db_name" to dbName,
                    "maxPoolSize" to 1,
                    "minPoolSize" to 1,
                    "connection_string" to "mongodb://localhost:27018/$dbName"
            )
        }

        val client = MongoClient.createShared(vertx, config)
        return client
    }

    fun insertDocument(data: JsonArray, collection:String, index:Int = 0, client: MongoClient, future: Future<Any>){
        client.insert(collection, data.getJsonObject(index), { res ->
            if (res.succeeded()) {
                if (index == data.size() - 1){
                    future.complete("Mongo INSERT Async to $collection")
                } else {
                    insertDocument(data, collection, index.inc(), client, future)
                }

            } else {
                res.cause().printStackTrace()
                future.fail(res.cause())
            }
        })
    }

    /**
     *  Step1 - Leitura de todos os registros da tabela
     **/
    fun listAllDocuments(client:MongoClient, collection:String, future:Future<Any>){

        val query = json {
            obj()
        }

        client.find(collection, query, {

            if (it.succeeded()) {
                future.complete("Mongo Full Scan Step1: read ${it.result().size} objects")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * Step 2 - leitura com apresentação de todas as colunas para o CPF = 211.211.224-21
     * */
    fun listFilteredByField(client:MongoClient, collection:String, field:String, value:String, future:Future<Any>){

        val query = json {
            obj(field to value)
        }

        client.find(collection, query, {

            if (it.succeeded()) {
//                future.complete("Mongo list filtered: read ${it.result().size} object(s) where field $field was $value")
                future.complete("Mongo ASYNC Step2: read ${it.result().size} object(s)")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * 3 - leitura com apresentação das colunas A, B e E para os
     * CPFs IN (122.432.104-14,324.431.124-02,443.112.312-00,144.323.134-11,314.120.111-20)
     */
    fun listFilteredByFieldWithSeveralValues(client:MongoClient, collection:String, queryField:String, values:JsonArray, displayFields:JsonArray,  future:Future<Any>){

        val query = json {
            obj(queryField to obj("\$in" to array(values)))
        }
        val displayFieldObj = json {
            obj{
                displayFields.forEach{
                    put(it as String, 1)
                }
            }
        }

        client.findWithOptions(collection, query, FindOptions(displayFieldObj), {

            if (it.succeeded()) {
//                future.complete("Mongo list filtered: read ${it.result().size} object(s) where queryField $queryField was ${values.joinToString(",","[", "]")}")
//                println((it.result()[0] as JsonObject).get("Nome Cliente"))
                future.complete("Mongo ASYNC Step3: read ${it.result().size} object(s) ")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * Step 4 - leitura com apresentação de todas as colunas para os contratos criados em 2015 ou seja Data do Contrato entre 01/01/2015 e 31/12/2015
     * */
    fun listFilteredByFieldWithBetween(client:MongoClient, collection:String, field:String, upperBound:Any, lowerBound:Any, future:Future<Any>){

        val query = json {
            obj(field to obj(
                    "\$gte" to lowerBound,
                    "\$lte" to upperBound))
        }

        client.find(collection, query, {

            if (it.succeeded()) {
                future.complete("Mongo Step4: read ${it.result().size} object(s)")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * Step 5 - leitura com apresentação TOTAL ( Principal ) dos contratos criados em 2016 ou seja Data do Contrato entre 01/01/2016 e 31/12/2016.
     * */
    fun listSumFieldWithBetween(client:MongoClient, collection:String, queryField:String, sumField:String, upperBound:Any, lowerBound:Any, future:Future<Any>){

        var command = json {
            obj(
                    "aggregate" to collection,
                    "pipeline" to array(
                            obj ( "\$match" to
                                    obj(queryField to
                                            obj(
                                                    "\$gte" to lowerBound,
                                                    "\$lte" to upperBound
                                            )
                                    )
                            ),
                            obj ("\$group" to
                                    obj(
                                            "_id" to null,
                                            "total" to obj("\$sum" to "\$$sumField")
                                    )
                            )
                    )
            )
        }

        client.runCommand("aggregate", command, {
            if (it.succeeded()) {
//                future.complete("Mongo SUM($sumField): value ${it.result().getJsonArray("result").get<JsonObject>(0).getDouble("total")}")
                future.complete("Mongo step5 done")
            } else {
                it.cause().printStackTrace()
                future.fail(it.cause().message)
            }
        })

    }

    /**
     * 6 - leitura com apresentação TOTAL ( Principal ) e TOTAL ( Interest ) agrupados por ANO.
     * */
    fun listSumFieldsWithGroupByThirdField(client:MongoClient, collection:String, groupField:String, sumField1:String, sumField2:String, future:Future<Any>){

        var command = json {
            obj(
                    "aggregate" to collection,
                    "pipeline" to array(
                            obj ("\$group" to
                                    obj(
                                            "_id" to "\$$groupField",
                                            "total_$sumField1" to  obj("\$sum" to "\$$sumField1"),
                                            "total_$sumField2" to  obj("\$sum" to "\$$sumField2")
                                    )
                            )
                    )
            )
        }

        client.runCommand("aggregate", command, {
            if (it.succeeded()) {
//                val year1 = it.result().getJsonArray("result").get<JsonObject>(0).getString("_id")
//                val year2 = it.result().getJsonArray("result").get<JsonObject>(1).getString("_id")
//                val total1y1 = it.result().getJsonArray("result").get<JsonObject>(0).getDouble("total_$sumField1")
//                val total2y1 = it.result().getJsonArray("result").get<JsonObject>(0).getDouble("total_$sumField2")
//                val total1y2 = it.result().getJsonArray("result").get<JsonObject>(0).getDouble("total_$sumField1")
//                val total2y2 = it.result().getJsonArray("result").get<JsonObject>(0).getDouble("total_$sumField2")
//                future.complete("Mongo step6 done: $year1 (total_$sumField1 = $total1y1, total_$sumField2 = $total2y1) ; $year2 (total_$sumField1 = $total1y2, total_$sumField2 = $total2y2)" )
                future.complete("Mongo step6 done")
            } else {
                it.cause().printStackTrace()
                future.fail(it.cause().message)
            }
        })

    }

    /**
     * Step 7 - leitura com apresentação de todas colunas para os contratos de 2016 com principal maior que 15000.
     * */
    fun step7(client:MongoClient, collection:String, queryField:String, valueQueryField:String, field2:String, lowerBoundField2:Any, future:Future<Any>){

        val query = json {
            obj(
                    queryField to valueQueryField,
                    field2 to obj("\$gte" to lowerBoundField2)
            )
        }

        client.find(collection, query, {

            if (it.succeeded()) {
//                future.complete("Mongo step 7: read ${it.result().size} object(s)")
                future.complete("Mongo step 7 done")
            } else {
                future.fail(it.cause().message)
            }

        })

    }
}