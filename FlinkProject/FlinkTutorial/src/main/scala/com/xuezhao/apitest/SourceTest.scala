package com.xuezhao.apitest


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import scala.util.Random


//定义样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据源 1、从集合中读取
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.11)

    )
    val stream1 = env.fromCollection(dataList)
    stream1.print()
    //2、从文件中读取数据
    val inputPath = "D:\\github\\DM_ML\\FlinkProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    //3、从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","test")
    val stream3 = env.addSource( new FlinkKafkaConsumer[String]("topic",new SimpleStringSchema(),properties) )
    //4、自定义source
    val stream4 = env.addSource( new SensorSource() )

    stream4.print()
    //执行
    env.execute("source test")
  }
}

//自定义SourceFunction
class SensorSource() extends SourceFunction[SensorReading]{
  //定义一个标识位，用来表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val rand = new Random()
    //随机生成一组10个传感器的初始温度
    var curTemp = 1.to(10).map( i =>("sensor_" + i, rand.nextDouble()*100))
    //定义无限循环，不停地产生数据，除非被canel
    while(running){
      //在上次数据机上微调更新温度值
      curTemp = curTemp.map(
        data => (data._1,data._2 + rand.nextGaussian())
        )
        //获取当前时间戳
        val curTime = System.currentTimeMillis()

        curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      //间隔100ms
      Thread.sleep(100)
    }

  }
}
