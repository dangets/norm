package com.dangets.norm.ws

import spark.kotlin.ignite

fun main(args: Array<String>) {
    val http = ignite()

    http.get("/status") { "All good" }
    http.get("/fileModel") { "must supply some query params" }
    http.get("/fileModel/:versionId") {
        val params = params(":versionId")
        println(params)
        //dataToJson()
    }

    //http.post("/fileModel/guess")
}