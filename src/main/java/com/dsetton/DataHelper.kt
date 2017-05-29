package com.dsetton

import com.aerospike.client.Bin
import com.arangodb.entity.BaseDocument
import com.arangodb.velocypack.VPackBuilder
import com.arangodb.velocypack.VPackSlice
import com.arangodb.velocypack.ValueType
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.OpenOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.RecordParser
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

object DataHelper {

    var jsonArray: JsonArray = JsonArray()
    var baseDocumentList:MutableList<BaseDocument> = mutableListOf()
    var vPackSliceList:MutableList<VPackSlice> = mutableListOf()
    var binList:MutableList<MutableList<Bin>> = mutableListOf()

    fun parseData(vertx:Vertx, lines:List<String>): Future<Void> {
        val fut = Future.future<Void>()

        val startDate = Date()

        jsonArray = JsonArray()

        vertx.executeBlocking<String>({
            try {
                val headerLine:String = lines[0]
                val dataLines = lines.subList(1, lines.size)

                val headers:List<String> = headerLine.split(";")

                dataLines.map{
                    var lineObj: JsonObject = JsonObject()
                    var baseDoc: BaseDocument = BaseDocument()
                    val builder = VPackBuilder()
                    builder.add(ValueType.OBJECT)

                    var aerospikeRecord:MutableList<Bin> = mutableListOf()

                    val data:List<String> = it.split(";")
                    val limit = headers.size-1
                    for (i in 0..limit){
                        val key = headers[i].trim().replace(".", "_")
                        val value = data.getOrElse(i, {""})
                        if (key.startsWith("Data", true)) {
                            val dateAsLong:Long = if ("" == value) 0 else Date.parse(value)
                            lineObj.put(key, dateAsLong)
                            baseDoc.addAttribute(key, dateAsLong)
                            builder.add(key, dateAsLong)
                            var binKey = key
                            if (binKey.length > 14) binKey = binKey.substring(0, 13)
                            val bin: Bin = Bin(binKey, dateAsLong)
                            aerospikeRecord.add(bin)

                        } else if ("Principal" == key || "Interest" == key) {
                            val valueAsNumber:Double = if ("" == value || "-" == value.trim()) 0.0 else value.replace(",", ".").toDouble()
                            lineObj.put(key, valueAsNumber)
                            baseDoc.addAttribute(key, valueAsNumber)
                            builder.add(key, valueAsNumber)
                            var binKey = key
                            if (binKey.length > 14) binKey = binKey.substring(0, 13)
                            val bin: Bin = Bin(binKey, valueAsNumber)
                            aerospikeRecord.add(bin)
                        }else {
                            lineObj.put(key, value)
                            baseDoc.addAttribute(key, value)
                            builder.add(key, value)
                            var binKey = key
                            if (binKey.length > 14) binKey = binKey.substring(0, 13)
                            val bin: Bin = Bin(binKey, value)
                            aerospikeRecord.add(bin)
                        }

                    }

                    builder.close()

                    jsonArray.add(lineObj)
                    baseDocumentList.add(baseDoc)
                    vPackSliceList.add(builder.slice())
                    binList.add(aerospikeRecord)
                }
                it.complete("JSON created in ${Date().time - startDate.time} ms")

            } catch (e:java.lang.Exception){
                e.printStackTrace()
                it.fail("Exception")
            }

        }, {
            if (it.succeeded()){
                println(it.result())
                fut.complete()
            } else {
                fut.fail(it.cause().message)
            }
        })

        return fut

    }

    fun loadFile(vertx: Vertx, filename:String): Future<List<String>> {
        val startDate = Date()
        val fut = Future.future<List<String>>()
        var lines:MutableList<String> = mutableListOf()

        var openOptions: OpenOptions = OpenOptions()
        openOptions.isRead = true

        var counter = AtomicInteger()

        val recordParser: RecordParser = RecordParser.newDelimited("\n", { rowBuffer ->
            val rowString:String = rowBuffer.toString().trim().replace("\t", ";")
            lines.add(rowString)
            counter.getAndIncrement()
        })

        vertx.fileSystem().open(filename, openOptions, { result ->
            if (result.succeeded()) {
                val asyncFile: AsyncFile = result.result()
                asyncFile.handler({buffer -> recordParser.handle(buffer)})
                asyncFile.endHandler({
                    println("File with ${lines.size} lines read in ${Date().time - startDate.time} ms")
                    fut.complete(lines)
                })
            } else {
                System.err.println("Oh oh ...${result.cause()}")
                fut.fail(result.cause())
            }
        })

        return fut
    }
}