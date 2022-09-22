package cn.xdc.bigdata.scala.core.wc

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost", 9999)
    val client1 = new Socket("localhost", 8888)
    val task = new Task()
    val subTask = new SubTask()
    val subTask1 = new SubTask()
    subTask.logic = task.logic
    subTask.datas = task.datas.take(2)
    subTask1.logic = task.logic
    subTask1.datas = task.datas.takeRight(2)
    val out = client.getOutputStream
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(subTask)
    objOut.flush()
    objOut.close()
    client.close()

    val out1 = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    objOut1.writeObject(subTask1)
    objOut1.flush()
    objOut1.close()
    client1.close()
  }
}
