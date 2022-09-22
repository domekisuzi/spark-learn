package cn.xdc.bigdata.scala.core.wc

class Task extends Serializable {

  val datas = List(1, 2, 3, 4)
  //  val logic = (num:Int)=>{num*2 }
  val logic: (Int) => Int = _ * 2


  //计算
  def compute() = {
    datas.map(logic)
  }


}

