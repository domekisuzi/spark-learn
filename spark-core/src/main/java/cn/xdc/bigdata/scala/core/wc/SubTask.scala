package cn.xdc.bigdata.scala.core.wc

class SubTask extends Serializable {

  var datas: List[Int] = _
  var logic:(Int)=>Int = _


  def compute() = {
    datas.map(logic)
  }

}
