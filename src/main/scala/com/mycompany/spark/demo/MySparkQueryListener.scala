package com.mycompany.spark.demo

import java.util.Date

import org.apache.spark.sql.streaming.StreamingQueryListener

class MySparkQueryListener extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val lastProgress = event.progress
    println(new Date().toString + s": Finished query ${event.progress.name}, batchId=${lastProgress.batchId}, " +
      s"numInputRows=${lastProgress.numInputRows}, processedRowsPerSecond=${lastProgress.processedRowsPerSecond}, " +
      s"durationMs=${lastProgress.durationMs.getOrDefault("triggerExecution", -1L)}, ")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

}
