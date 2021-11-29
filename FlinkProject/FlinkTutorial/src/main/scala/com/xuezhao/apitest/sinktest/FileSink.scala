package com.xuezhao.apitest.sinktest

import com.xuezhao.apitest.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "D:\\github\\DM_ML\\FlinkProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream
      .map(data=> {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    dataStream.print()
//    dataStream.writeAsCsv("D:\\github\\DM_ML\\FlinkProject\\FlinkTutorial\\src\\main\\resources\\out.txt")
    dataStream.addSink(StreamingFileSink.forRowFormat(new Path("D:\\github\\DM_ML\\FlinkProject\\FlinkTutorial\\src\\main\\resources\\out1.txt")
    ,new SimpleStringEncoder[SensorReading]()).build() )
    env.execute("file sink test")
  }
}
