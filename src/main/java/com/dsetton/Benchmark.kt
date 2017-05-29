package com.dsetton

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


class Benchmark : AbstractVerticle(){

    val dbName = "poc-".plus(Date().time)
    var dbNameSync = "poc-".plus(Date().time).plus("SYNC")
    var dbNameAsync = "poc-".plus(Date().time).plus("ASYNC")
    val collectionName = "pocDB"
    var collectionNameSync = collectionName.plus("SYNC").plus(Date().time)
    var collectionNameAsync = collectionName.plus("ASYNC").plus(Date().time)

    override fun stop(){
        println("STOPPING")
        MongoHelper.getClient(vertx, dbName).close()
//        AerospikeHelper.client.close()
//        AerospikeHelper.asyncClient.close()
    }

    override fun start(){
        println("Start")
        System.setProperty("org.mongodb.async.type", "netty")
        val results:MutableMap<Int, MutableMap<String, Long>> = mutableMapOf()
        var currentRound = AtomicInteger(1)

        results.put(currentRound.get(), mutableMapOf())
        runBenchmark(currentRound.get(), results)
        .compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        }).compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        }).compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        }).compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        }).compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        }).compose({
            results.put(currentRound.incrementAndGet(), mutableMapOf())
            println("Current Round: $currentRound")
            runBenchmark(currentRound.get(), results)
        })
        .setHandler({
            results.forEach({ round ->
                print("Round ${round.key}; ")
                round.value.forEach({
                    println("${it.key} ; ${it.value}")
                    print("${it.value};")
                })
                print("\n")
            })
            vertx.close()
        })

    }

    private fun runBenchmark(round:Int = 0, results:MutableMap<Int, MutableMap<String, Long>>):Future<Any>{

        dbNameSync = "poc-".plus(Date().time).plus("SYNC").plus("-round$round")
        dbNameAsync = "poc-".plus(Date().time).plus("ASYNC").plus("-round$round")
        collectionNameSync = collectionName.plus("SYNC").plus(Date().time).plus("round$round")
        collectionNameAsync = collectionName.plus("ASYNC").plus(Date().time).plus("round$round")

        val configObj = vertx.orCreateContext.config()

        val FILENAME = configObj.getString("filename")
        val step2Field = configObj.getString("step2Field")
        val step2Value = configObj.getString("step2Value")
        val step3Field = configObj.getString("step3Field")
        val step3Values:JsonArray = configObj.getJsonArray("step3Values")
        val step3DisplayFields:JsonArray = configObj.getJsonArray("step3DisplayFields")
        val step4Field = configObj.getString("step4Field")
        val step4LowerBound = Date.parse(configObj.getString("step4LowerBound"))
        val step4UpperBound = Date.parse(configObj.getString("step4UpperBound"))
        val step5LowerBound = Date.parse(configObj.getString("step5LowerBound"))
        val step5UpperBound = Date.parse(configObj.getString("step5UpperBound"))
        val step5QueryField = configObj.getString("step5QueryField")
        val step5Field = configObj.getString("step5Field")
        val step6GroupField:String = configObj.getString("step6GroupField")
        val step6sumField1:String = configObj.getString("step6sumField1")
        val step6sumField2:String = configObj.getString("step6sumField2")
        val step7QueryField:String = configObj.getString("step7QueryField")
        val step7ValueQueryField:String = configObj.getString("step7ValueQueryField")
        val step7Field2:String = configObj.getString("step7Field2")
        val step7LowerBoundField2:Any = configObj.getFloat("step7LowerBoundField2")

        val startFuture = Future.future<Any>()
        val fut = Future.future<Any>()
        var startDate = System.currentTimeMillis()

        val loadFileFuture = DataHelper.loadFile(vertx, FILENAME)

        loadFileFuture.compose<Void>({
            startDate = System.currentTimeMillis()
            val lines:List<String> = it
            val dataFuture = DataHelper.parseData(vertx, lines)
            return@compose dataFuture
        }).compose({
            ArangoHelper.prepareDBSync(vertx, dbNameSync, collectionNameSync)
        }).compose({
            ArangoHelper.prepareDBAsync(dbNameAsync, collectionNameAsync)
        }).compose<Any>({
            startDate = System.currentTimeMillis()
            return@compose mongoInsert(dbNameAsync, collectionNameAsync)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoInsertSync(dbNameSync, collectionNameSync)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoInsertAsync(dbNameAsync, collectionNameAsync)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aerospikeInsertSync()
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aerospikeInsertAsync()
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoListAll(dbNameAsync, collectionNameAsync)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoReadSync(dbNameSync, collectionNameSync)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoReadAsync(dbNameAsync, collectionNameAsync)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aerospikeScanSync()
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep2(dbNameAsync, collectionNameAsync, step2Field, step2Value)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep2(dbNameSync, collectionNameSync, step2Field, step2Value)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aeroStep2(step2Field, step2Value)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep3(dbNameAsync, collectionNameAsync, step3Field, step3Values, step3DisplayFields)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep3(dbNameSync, collectionNameSync, step3Field, step3Values, step3DisplayFields)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aeroStep3(step3Field, step3Values, step3DisplayFields)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep4(dbNameAsync, collectionNameAsync, step4Field, step4UpperBound, step4LowerBound)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep4(dbNameSync, collectionNameSync, step4Field, step4UpperBound, step4LowerBound)
//        }).compose({
//            println("$it in ${System.currentTimeMillis() - startDate} ms")
//            startDate = System.currentTimeMillis()
//            aeroStep4(step4Field, step4UpperBound, step4LowerBound)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep5(dbNameAsync, collectionNameAsync, step5QueryField, step5Field, step5UpperBound, step5LowerBound)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep5(dbNameSync, collectionNameSync, step5QueryField, step5Field , step5UpperBound, step5LowerBound)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aeroStep5(step5QueryField, step5Field, step5UpperBound, step5LowerBound)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep6(dbNameAsync, collectionNameAsync, step6GroupField, step6sumField1, step6sumField2)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep6(dbNameSync, collectionNameSync, step6GroupField, step6sumField1, step6sumField2)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aeroStep6(step6GroupField, step6sumField1, step6sumField2)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            mongoStep7(dbNameAsync, collectionNameAsync, step7QueryField, step7ValueQueryField, step7Field2, step7LowerBoundField2)
        }).compose({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            startDate = System.currentTimeMillis()
            arangoStep7(dbNameSync, collectionNameSync, step7QueryField, step7ValueQueryField, step7Field2, step7LowerBoundField2)
//        }).compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            aeroStep7(step7QueryField, step7ValueQueryField, step7Field2, step7LowerBoundField2)
        }).setHandler({
            val elapsedTime = System.currentTimeMillis() - startDate
            results[round]!!.put(it.toString(), elapsedTime)
            println("$it in $elapsedTime ms")
            fut.complete()
        })
//        .compose({
//            val elapsedTime = System.currentTimeMillis() - startDate
//            results[round]!!.put(it.toString(), elapsedTime)
//            println("$it in $elapsedTime ms")
//            startDate = System.currentTimeMillis()
//            startFuture.complete()
//        }, startFuture)

        startFuture.setHandler({
            fut.complete()
        })

        return fut
    }


    private fun mongoInsert(dbName:String, collectionName:String): Future<Any>? {
        val mongoInsertFuture = Future.future<Any>()
        MongoHelper.insertDocument(DataHelper.jsonArray, collectionName, 0, MongoHelper.getClient(vertx, dbName), mongoInsertFuture)
        return mongoInsertFuture
    }

    private fun mongoListAll(dbName:String, collectionName:String): Future<Any>? {
        val mongoReadFuture = Future.future<Any>()
        MongoHelper.listAllDocuments(MongoHelper.getClient(vertx, dbName), collectionName, mongoReadFuture)
        return mongoReadFuture
    }

    private fun mongoStep2(dbName:String, collectionName:String, field:String, value:String): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.listFilteredByField(MongoHelper.getClient(vertx, dbName), collectionName, field, value, future)
        return future
    }

    private fun mongoStep3(dbName:String, collectionName:String, field:String, values:JsonArray, displayFields:JsonArray): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.listFilteredByFieldWithSeveralValues(MongoHelper.getClient(vertx, dbName), collectionName, field, values, displayFields, future)
        return future
    }

    private fun mongoStep4(dbName:String, collectionName:String, field:String, upperBound:Any, lowerBound:Any): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.listFilteredByFieldWithBetween(MongoHelper.getClient(vertx, dbName), collectionName, field, upperBound, lowerBound, future)
        return future
    }

    private fun mongoStep5(dbName:String, collectionName:String, queryField:String, sumField:String, upperBound:Any, lowerBound:Any): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.listSumFieldWithBetween(MongoHelper.getClient(vertx, dbName), collectionName, queryField, sumField, upperBound, lowerBound, future)
        return future
    }

    private fun mongoStep6(dbName:String, collectionName:String, groupField:String, sumField1:String, sumField2:String): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.listSumFieldsWithGroupByThirdField(MongoHelper.getClient(vertx, dbName), collectionName, groupField, sumField1, sumField2, future)
        return future
    }

    private fun mongoStep7(dbName:String, collectionName:String, queryField:String, valueQueryField:String, field2:String, lowerBoundField2:Any): Future<Any>? {
        val future = Future.future<Any>()
        MongoHelper.step7(MongoHelper.getClient(vertx, dbName), collectionName, queryField, valueQueryField, field2, lowerBoundField2, future)
        return future
    }

    private fun arangoInsertSync(dbName:String, collectionName:String): Future<Any>? {
        val arangoInsertFuture = Future.future<Any>()

        ArangoHelper.insertJsonSync(vertx, dbName, collectionName, DataHelper.jsonArray, 0, ArangoHelper.arangoDBSync, arangoInsertFuture)
        return arangoInsertFuture
    }

    private fun arangoInsertAsync(dbName:String, collectionName:String): Future<Any>? {
        val arangoInsertFuture = Future.future<Any>()
        ArangoHelper.insertJsonAsync(dbName, collectionName, DataHelper.jsonArray, 0, ArangoHelper.arangoDBAsync, arangoInsertFuture)
        return arangoInsertFuture
    }

    private fun arangoReadAsync(dbName:String, collectionName:String): Future<Any>? {
        val arangoFuture = Future.future<Any>()
        ArangoHelper.fullScanAsync(vertx, dbName, collectionName, ArangoHelper.arangoDBAsync, arangoFuture)
        return arangoFuture
    }

    private fun arangoReadSync(dbName:String, collectionName:String): Future<Any>? {
        val arangoFuture = Future.future<Any>()
        ArangoHelper.fullScanJsonSync(vertx, dbName, collectionName, ArangoHelper.arangoDBSync, arangoFuture)
        return arangoFuture
    }

    private fun arangoStep2(dbName:String, collectionName:String,  field: String, value: String): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step2Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, field, value, future)
        return future
    }

    private fun arangoStep3(dbName:String, collectionName:String,  field:String, values:JsonArray, displayFields:JsonArray): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step3Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, field, values, displayFields, future)
        return future
    }

    private fun arangoStep4(dbName:String, collectionName:String,  field:String, upperBound:Any, lowerBound:Any): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step4Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, field, upperBound, lowerBound, future)
        return future
    }

    private fun arangoStep5(dbName:String, collectionName:String,  queryField:String, sumField:String, upperBound:Any, lowerBound:Any): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step5Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, queryField, sumField, upperBound, lowerBound, future)
        return future
    }

    private fun arangoStep6(dbName:String, collectionName:String,  groupField:String, sumField1:String, sumField2:String): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step6Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, groupField, sumField1, sumField2, future)
        return future
    }

    private fun arangoStep7(dbName:String, collectionName:String,  queryField:String, valueQueryField:String, field2:String, lowerBoundField2:Any): Future<Any>? {
        val future = Future.future<Any>()
        ArangoHelper.step7Sync(vertx, ArangoHelper.arangoDBSync, dbName, collectionName, queryField, valueQueryField, field2, lowerBoundField2, future)
        return future
    }

    private fun aerospikeInsertSync(): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.insertBinSync(vertx, collectionNameSync, DataHelper.binList, 0, AerospikeHelper.client, future)
        return future
    }

    private fun aerospikeInsertAsync(): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.insertBinAsync(vertx, collectionNameAsync, DataHelper.binList, 0, AerospikeHelper.asyncClient, future)
        return future
    }

    private fun aerospikeScanSync(): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.scanSync(vertx, collectionNameSync, AerospikeHelper.client, future)
        return future
    }

    private fun aeroStep2(field: String, value: String): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step2Sync(vertx, AerospikeHelper.client, collectionNameSync, field, value, future)
        return future
    }

    private fun aeroStep3(queryField:String, values:List<String>, displayFields:List<String>): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step3Sync(vertx, AerospikeHelper.client, collectionNameSync, queryField, values, displayFields, future)
        return future
    }

    private fun aeroStep4(field:String, upperBound:Long, lowerBound:Long): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step4Sync(vertx, AerospikeHelper.client, collectionNameSync, field, upperBound, lowerBound, future)
        return future
    }

    private fun aeroStep5(field:String, sumField: String, upperBound:Long, lowerBound:Long): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step5Sync(vertx, AerospikeHelper.client, collectionNameSync, field, sumField, upperBound, lowerBound, future)
        return future
    }

    private fun aeroStep6(binName:String, sumBin1:String, sumBin2:String): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step6Sync(vertx, AerospikeHelper.client, collectionNameSync, binName, sumBin1, sumBin2, future)
        return future
    }

    private fun aeroStep7(binQuery:String, valueQueryField:String, field2:String, lowerBoundField2:Any): Future<Any>? {
        val future = Future.future<Any>()
        AerospikeHelper.step7Sync(vertx, AerospikeHelper.client, collectionNameSync, binQuery, valueQueryField, field2, lowerBoundField2, future)
        return future
    }





}