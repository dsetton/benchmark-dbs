package com.dsetton

import com.aerospike.client.*
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.async.AsyncClientPolicy
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy.Policy
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.query.*
import com.aerospike.client.task.IndexTask
import io.vertx.core.Future
import io.vertx.core.Vertx
import com.aerospike.client.Record
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.task.RegisterTask
import kotlin.coroutines.experimental.EmptyCoroutineContext.plus


/**
 * Created by daniel on 17/05/17.
 */
object AerospikeHelper {

    val aerospikeWritePolicy: WritePolicy = WritePolicy()
    val aerospikeReadPolicy: Policy = Policy()
    val aerospikeAsyncWritePolicy:WritePolicy = AsyncClientPolicy().asyncWritePolicyDefault

    val client: AerospikeClient
        get() = AerospikeClient("127.0.0.1", 3000)

    val asyncClient: AsyncClient
        get() = AsyncClient("127.0.0.1", 3000)

    fun insertBinSync(vertx: Vertx, collectionName:String, data:List<List<Bin>>, index:Int = 0, client: AerospikeClient, future: Future<Any>){

        vertx.executeBlocking<Any>({
            val record = data[index]
            val aerospikeKey = Key("test", collectionName, record[0].value.toString().plus(index))
            client.put(aerospikeWritePolicy, aerospikeKey, *record.toTypedArray() )
            it.complete()
        }, {
            if (!it.failed()) {
                if (index == data.size - 1){
                    future.complete("Aerospike INSERT SYNC to $collectionName")
                } else {
                    insertBinSync(vertx, collectionName, data, index.inc(), client, future)
                }
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })
    }

    fun insertBinAsync(vertx: Vertx, collectionName:String, data:List<List<Bin>>, index:Int = 0, client: AsyncClient, future: Future<Any>){

        val record = data[index]
        val aerospikeKey = Key("test", collectionName, record[0].value.toString().plus(index))

        val internalFuture = Future.future<Any>()

        client.put(aerospikeAsyncWritePolicy,
                WriteHandler(internalFuture, client, aerospikeAsyncWritePolicy, aerospikeKey, *record.toTypedArray()),
                aerospikeKey, *record.toTypedArray())

        internalFuture.setHandler({
            if(it.succeeded()){
                if (index == data.size - 1){
                        future.complete("Aerospike INSERT ASYNC to $collectionName")
                    } else {
                        insertBinAsync(vertx, collectionName, data, index.inc(), client, future)
                    }

            } else {
                future.fail(internalFuture.cause())
            }
        })
    }

    fun scanSync(vertx: Vertx, collectionName:String, client: AerospikeClient, future: Future<Any>){

        val scanSyncCallback = ScanSyncCallback()
        vertx.executeBlocking<Record>({
            client.scanAll(client.scanPolicyDefault, "test", collectionName, scanSyncCallback)
            it.complete()
        }, {
            if (!it.failed()) {
                future.complete("Aerospike SYNC Full Scan: read ${scanSyncCallback.recordCount} objects")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })
    }

    /**
     * Step 2 - leitura com apresentação de todas as colunas para o CPF = 211.211.224-21
     * */
    fun step2Sync(vertx: Vertx, client: AerospikeClient, collectionName:String, field: String, value: String, future: Future<Any>){

//        val scanSyncCallback = ScanSyncCallback()
        vertx.executeBlocking<RecordSet>({
            //"select $field in test.$collectionName"
            var statement = Statement()
            statement.namespace = collectionName
            statement.setBinNames(field)
            statement.filter = Filter.equal(field, value)
            val recordSet = client.query(client.queryPolicyDefault, statement)
            it.complete(recordSet)
        }, {
            if (!it.failed()) {
                future.complete("Aerospike SYNC list filtered: read ${it.result()} objects")
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
    fun step3Sync(vertx: Vertx, client: AerospikeClient, collectionName:String, queryField:String, values:List<String>, displayFields:List<String>, future: Future<Any>){

        vertx.executeBlocking<RecordSet>({
            var statement = Statement()
            statement.namespace = "test"
            statement.setName = collectionName
//            statement.setBinNames(*displayFields.toTypedArray())
//            statement.filter = Filter.contains(queryField, IndexCollectionType.LIST, )
            var predicates:Array<PredExp> = arrayOf()
            values.forEach {
                predicates = predicates.plus(PredExp.stringBin(queryField))
                predicates = predicates.plus(PredExp.stringValue(it))
                predicates = predicates.plus(PredExp.stringEqual())
            }
            predicates = predicates.plus(PredExp.or(values.size))
            statement.setPredExp(*predicates)
            val recordSet = client.query(client.queryPolicyDefault, statement)
            it.complete(recordSet)
        }, {
            if (!it.failed()) {
//                future.complete("Aerospike SYNC Step 3: read ${it.result()} objects")
                future.complete("Aerospike SYNC Step 3 done")
            } else {
                it.cause().printStackTrace()
                future.complete(it.cause().message)
            }

        })
    }

    /**
     * Step 4 - leitura com apresentação de todas as colunas para os contratos criados em 2015 ou seja Data do Contrato entre 01/01/2015 e 31/12/2015
     * */
    fun step4Sync(vertx: Vertx, client: AerospikeClient, setName:String, binName:String, upperBound:Long, lowerBound:Long, future: Future<Any>){

        val binNameWithUpTo14Char = if (binName.length > 14) binName.substring(0, 13) else binName

        vertx.executeBlocking<IndexTask>({
            val indexTask = client.createIndex(null, "test", setName, binName.plus("Index"), binNameWithUpTo14Char, IndexType.NUMERIC)
            it.complete(indexTask)
        }, {
            vertx.executeBlocking<RecordSet>({ rs ->
                var statement = Statement()
                statement.namespace = "test"
                statement.setName = setName

                statement.setBinNames(binNameWithUpTo14Char)
                statement.filter = Filter.range(binNameWithUpTo14Char, lowerBound, upperBound)
                val recordSet = client.query(client.queryPolicyDefault, statement)
                rs.complete(recordSet)
            }, {
                if (!it.failed()) {
                    val rs = it.result()
                    val count:AtomicInteger = AtomicInteger()
                    rs.use { rs ->
                        while (rs.next()) {
                            count.getAndIncrement()
                        }
                    }
                    future.complete("Aerospike SYNC Step 4 done: read $count objects")
                } else {
                    it.cause().printStackTrace()
                    future.complete(it.cause().message)
                }

            })
        })

    }

    /**
     * Step 5 - leitura com apresentação TOTAL ( Principal ) dos contratos criados em 2016 ou seja Data do Contrato entre 01/01/2016 e 31/12/2016.
     * */
    fun step5Sync(vertx: Vertx, client: AerospikeClient, setName:String, binName:String, sumBin:String, upperBound:Long, lowerBound:Long, future: Future<Any>){

        val binNameWithUpTo14Char = if (binName.length > 14) binName.substring(0, 13) else binName

        vertx.executeBlocking<IndexTask>({
            val indexTask = client.createIndex(null, "test", setName, binName.plus("Index"), binNameWithUpTo14Char, IndexType.NUMERIC)
            it.complete(indexTask)
        }, {
            vertx.executeBlocking<RegisterTask>({ t ->
                val task = client.register(null, "udf/sum-bin.lua", "sum-bin.lua", Language.LUA)
                t.complete(task)
            },{
                vertx.executeBlocking<RecordSet>({ r ->
                    var statement = Statement()
                    statement.namespace = "test"
                    statement.setName = setName
                    statement.setBinNames(binNameWithUpTo14Char)
                    statement.filter = Filter.range(binNameWithUpTo14Char, lowerBound, upperBound)
                    statement.setAggregateFunction("sum-bin", "sum_single_bin", Value.get(sumBin))
                    val recordSet = client.query(client.queryPolicyDefault, statement)
                    r.complete(recordSet)
                }, {
                    if (!it.failed()) {
                        val rs = it.result()
                        vertx.executeBlocking<AtomicInteger>({ at ->
                            val count:AtomicInteger = AtomicInteger()
                            rs.use { rs ->
                                while (rs.next()) {
                                    count.getAndIncrement()
                                }
                            }
                            at.complete(count)
                        }, {
                            future.complete("Aerospike SYNC Step 5 done: read $it objects")
                        })
                    } else {
                        it.cause().printStackTrace()
                        future.complete(it.cause().message)
                    }

                })
            })

        })

    }

    /**
     * 6 - leitura com apresentação TOTAL ( Principal ) e TOTAL ( Interest ) agrupados por ANO.
     * */
    fun step6Sync(vertx: Vertx, client: AerospikeClient, setName:String, binName:String, sumBin1:String, sumBin2:String, future: Future<Any>){

        val binNameWithUpTo14Char = if (binName.length > 14) binName.substring(0, 13) else binName
        val secondBinNameWithUpTo14Char = if (sumBin1.length > 14) sumBin1.substring(0, 13) else sumBin1
        val thirdBinNameWithUpTo14Char = if (sumBin2.length > 14) sumBin2.substring(0, 13) else sumBin2

//        vertx.executeBlocking<IndexTask>({
//            val indexTask1 = client.createIndex(null, "test", setName, binName.plus("Index"), binNameWithUpTo14Char, IndexType.STRING)
//            val indexTask2 = client.createIndex(null, "test", setName, sumBin1.plus("Index"), secondBinNameWithUpTo14Char, IndexType.NUMERIC)
//            val indexTask3 = client.createIndex(null, "test", setName, sumBin2.plus("Index"), thirdBinNameWithUpTo14Char, IndexType.NUMERIC)
//            it.complete()
//        }, {
            vertx.executeBlocking<RegisterTask>({
                val task = client.register(null, "udf/group-and-sum-bin.lua", "group-and-sum-bin.lua", Language.LUA)
                it.complete(task)
            },{
                vertx.executeBlocking<RecordSet>({
                    var statement = Statement()
                    statement.namespace = "test"
                    statement.setName = setName
//                    statement.setBinNames(arrayOf(binNameWithUpTo14Char, secondBinNameWithUpTo14Char, thirdBinNameWithUpTo14Char))
//                    statement.filter = Filter.range(binNameWithUpTo14Char, lowerBound, upperBound)
                    var functionArgs = mutableListOf<Value>()
                    functionArgs.add(Value.get(binNameWithUpTo14Char))
                    functionArgs.add(Value.get(secondBinNameWithUpTo14Char))
                    functionArgs.add(Value.get(thirdBinNameWithUpTo14Char))
                    statement.setAggregateFunction("group-and-sum-bin", "sum_by_year", *functionArgs.toTypedArray())
                    val recordSet = client.query(client.queryPolicyDefault, statement)
                    it.complete(recordSet)
                }, {
                    if (!it.failed()) {
                        future.complete("Aerospike SYNC Step 6 done")
                        val rs = it.result()
                        val count:AtomicInteger = AtomicInteger()
                        rs.use { rs ->
                            while (rs.next()) {
                                count.getAndIncrement()
                            }
                        }
//                        println("console: Aerospike SYNC Step 6: read $count objects")
//                        future.complete("Aerospike SYNC Step 6 done: read $count objects")
                    } else {
                        it.cause().printStackTrace()
                        future.complete(it.cause().message)
                    }

                })
            })

//        })

    }

    /**
     * Step 7 - leitura com apresentação de todas colunas para os contratos de 2016 com principal maior que 15000.
     * */
    fun step7Sync(vertx: Vertx, client: AerospikeClient, setName:String, binQuery:String, valueQueryField:String, field2:String, lowerBoundField2:Any, future: Future<Any>){

        val binNameWithUpTo14Char = if (binQuery.length > 14) binQuery.substring(0, 13) else binQuery
        val secondBinNameWithUpTo14Char = if (field2.length > 14) field2.substring(0, 13) else field2

        if (lowerBoundField2 is Int){
            vertx.executeBlocking<RecordSet>({
                var statement = Statement()
                statement.namespace = "test"
                statement.setName = setName
//            statement.setBinNames(*displayFields.toTypedArray())
//            statement.filter = Filter.contains(queryField, IndexCollectionType.LIST, )
                var predicates:Array<PredExp> = arrayOf()
                predicates = predicates.plus(PredExp.stringBin(binNameWithUpTo14Char))
                predicates = predicates.plus(PredExp.stringValue(valueQueryField))
                predicates = predicates.plus(PredExp.stringEqual())
                predicates = predicates.plus(PredExp.integerBin(secondBinNameWithUpTo14Char))
                predicates = predicates.plus(PredExp.integerValue(lowerBoundField2.toLong()))
                predicates = predicates.plus(PredExp.integerGreaterEq())
                predicates = predicates.plus(PredExp.and(2))
                statement.setPredExp(*predicates)
                val recordSet = client.query(client.queryPolicyDefault, statement)
                it.complete(recordSet)
            }, {
                if (!it.failed()) {
                    val count:AtomicInteger = AtomicInteger()
                    it.result().use { rs ->
                        while (rs.next()) {
                            count.getAndIncrement()
                        }
                    }
                    future.complete("Aerospike SYNC Step 7 done: read $count objects")
                } else {
                    it.cause().printStackTrace()
                    future.complete(it.cause().message)
                }

            })
        } else {
            future.fail("Not Long")
        }
    }

}

class WriteHandler(val future: Future<Any>, val client:AsyncClient, val policy:WritePolicy, val key:Key, vararg bin:Bin) : WriteListener {
    override fun onSuccess(key: Key?) {
        future.complete()
    }

    override fun onFailure(e: AerospikeException?) {
        future.fail(e.toString())
    }
}

class ScanSyncCallback : ScanCallback{
    var recordCount = 0
    override fun scanCallback(key: Key?, record: Record?) {
        recordCount = recordCount.inc()
//        println(recordCount)
    }

}