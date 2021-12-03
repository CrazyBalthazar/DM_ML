package com.xuezhao.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\github\\DM_ML\\FlinkProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream
      .map(data=> {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    //每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataStream
      .map( data => (data.id, data.temperature, data.timestamp) )
      .keyBy(_._1)//按照二元组的第一个元素分组
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(100))) //滚动窗口
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//      .countWindow(10)
//      .minBy(1)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))


    resultStream.print()

    env.execute("window test")
  }
}

class MyReducer extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timestamp,t.temperature.min(t1.temperature))
  }
}
