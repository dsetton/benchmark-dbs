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
     * Step 2 - leitura com apresentação de todas as colunas para um campo específico
     * */
    fun listFilteredByField(client:MongoClient, collection:String, field:String, value:String, future:Future<Any>){

        val query = json {
            obj(field to value)
        }

        client.find(collection, query, {

            if (it.succeeded()) {
                future.complete("Mongo ASYNC Step2: read ${it.result().size} object(s)")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * 3 - leitura com apresentação das colunas A, B e E para um campo com vários valores (IN)
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
                future.complete("Mongo ASYNC Step3: read ${it.result().size} object(s) ")
            } else {
                future.fail(it.cause().message)
            }

        })

    }

    /**
     * Step 4 - leitura com apresentação de todas as colunas, filtradas por campo com range de valores (between ou gte && lte)
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
     * Step 5 - leitura com apresentação TOTAL/SUM ( campo ) dos dados dentro de um range
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
                future.complete("Mongo step5 done")
            } else {
                it.cause().printStackTrace()
                future.fail(it.cause().message)
            }
        })

    }

    /**
     * 6 - leitura com apresentação TOTAL ( campo 1 ) e TOTAL ( campo 2 ) agrupados por campo 3.
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
                future.complete("Mongo step6 done")
            } else {
                it.cause().printStackTrace()
                future.fail(it.cause().message)
            }
        })

    }

    /**
     * Step 7 - leitura com apresentação de todas colunas para os contratos de um range de campo 1 e com campo 2 maior que valor especificado.
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
                future.complete("Mongo step 7 done")
            } else {
                future.fail(it.cause().message)
            }

        })

    }
}
