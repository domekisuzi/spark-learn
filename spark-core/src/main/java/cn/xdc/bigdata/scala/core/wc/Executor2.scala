package cn.xdc.bigdata.scala.core.wc

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor2 {
  def main(args: Array[String]): Unit = {
    //启动服务器,接受数据
    val server = new ServerSocket(8888)
    println("服务器启动,接受数据 ")
    val client = server.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task = objIn.readObject().asInstanceOf[SubTask]
    val ints = task
      .compute()
    println("计算的结果为{8888}" + ints)
    objIn.close()
    client.close()
    server.close()
  }
}
