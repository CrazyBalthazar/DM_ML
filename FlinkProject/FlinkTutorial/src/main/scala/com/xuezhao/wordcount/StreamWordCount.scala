package com.xuezhao.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数据源
    val paramsTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramsTool.get("host")
    val port: Int = paramsTool.getInt("port")

    //接收socket文本流
    val inputDataStream = env.socketTextStream(host,port)
    //进行数据转换
    val resultDataStream = inputDataStream
      .flatMap{_.split(" ")}
      .map{(_,1)}
      .filter(_!=null)
      .keyBy(0)
      .sum(1)

    resultDataStream.print()
    //启动任务
    env.execute("StreamWordCount")
  }

}
