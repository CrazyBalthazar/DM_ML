package com.xuezhao.apitest

import org.apache.flink.api.common.functions.{ReduceFunction, RichFunction}
import org.apache.flink.streaming.api.scala._

object TransformTet {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "D:\\DM_ML\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream
      .map(data=> {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    //分组聚合，输出每个传感器当前最小值
    val aggStream = dataStream
      .keyBy("id")
      .minBy("temperature")
    //输出当前最小的温度值，以及最近的时间戳
    val resultStream = dataStream
      .keyBy("id")
//      .reduce((curSate,newData) =>
//        SensorReading( curSate.id, newData.timestamp, curSate.temperature.min(newData.temperature))
//      )
      .reduce(new MyReduceFunction)

    //多流转换
    //分流 将传感器温度数据分为低温和高温两条流
    val splitStream = dataStream

    resultStream.print()

    env.execute("transform test")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
}
