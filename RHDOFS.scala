import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

object RHDOFS {

  def getTuple(x:(Long,(String,String))):(Int,(Double,Double))={
    val cls = x._2._2.toInt
    val f = x._2._1.toDouble
    (cls,(f,f))
  }

  def isPos(x:Int):Double={
    val res = Math.log(x)/Math.log(2)
    var flag = 1.0
    if (res-res.round!=0.0){
      flag=0.0
    }
    flag
  }

  def getDepen(x: mutable.HashMap[Int,Int],mode:Set[Int]):Double={
    var col = Int.MaxValue
    for (ind <- mode){
      col = col & x(ind)
    }
    isPos(col)
  }

  def nonSignf(rdd:RDD[(String,mutable.HashMap[Int,Int])], mode:Set[Int], fi:Int, n:Double, k:Int):Set[Int]={
    var BB:Set[Int] = Set()
    for(i<-1 to k){
      var B:Set[Int] = Set()
      var R = mode
      var T = mode
      T = T.-(fi)
      while(T.nonEmpty){
        val f_ind = Random.nextInt(T.size)
        val TArr = T.toArray
        val f = TArr(f_ind)
        if (sig(rdd,R,f,n)==0.0){
          B = B.+(f)
          R = R.-(f)
        }
        T = T.-(f)
      }
      if (B.size>BB.size){
        BB=B
      }
    }
    BB
  }

  def sig(rdd:RDD[(String,mutable.HashMap[Int,Int])],mode:Set[Int],fi:Int,n:Double):Double={
    val res = rdd.map(x=>{
      val map = x._2
      var D_F = Int.MaxValue
      var fv = Int.MaxValue

      for (ind <- mode){
        if (ind!=fi){
          D_F = D_F & map(ind)
        }
        else{
          fv = map(ind)
        }
      }
      val d_F = isPos(D_F)
      val d_B = isPos(D_F & fv)
      d_B-d_F
    }).reduce(_+_)
    res/n
  }

  def info_BN(rhmats:RDD[(String,mutable.HashMap[Int,Int])],data:RDD[(String,(mutable.HashMap[Int,Double],String))],mode:Set[Int],
              fi:Int, centers: mutable.HashMap[Int, mutable.HashMap[String,Double]],n:Double):Double={
    val dataMem = rhmats.join(data).map(
      x=>{
        val X = x._2
        val cls = X._2._2
        val map = x._2._1
        var res = Int.MaxValue
        for (ind<-mode){
          if (ind!=fi){
            res = res & map(ind)
          }
        }

        var mem = 0.0
        if (isPos(res)<1.0){
          val disCls:mutable.HashMap[String,Double] = mutable.HashMap()
          val dataMap = X._2._1
          for (ind<-mode){
            if (ind!=fi){
              val value = dataMap(ind)
              val center = centers.getOrElse(ind,null)
              for (centerCls<-center){
                if (!disCls.keySet.contains(centerCls._1)){
                  val dis2 = Math.pow(value-centerCls._2,2.0)
                  disCls.put(centerCls._1,dis2)
                }
                else {
                  var dis2 = disCls.getOrElse(centerCls._1,0.0)
                  dis2 += Math.pow(value-centerCls._2,2.0)
                  disCls.put(centerCls._1,dis2)
                }
              }
            }
          }
          var fenMu = 0.0
          for (dis<-disCls){
            fenMu+=(1.0/dis._2)
          }
          mem = (1.0/disCls.getOrElse(cls,0.0))/fenMu
        }
        mem/n
      }).reduce(_+_)
    dataMem
  }

  def main(args: Array[String]): Unit = {
//    println("input the filepath:")
    val path = args(0)
    val classpath = path+"class/"
    val AttrNum = args(1).toInt
//    println("input the number of instances:")
    val n = args(2).toInt
//    println("input the number of Partitions:")
    val partitionNum = args(3).toInt
    val conf = new SparkConf().setAppName("RHDOFS").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var dep_mean = 0.0
    var dep_Set = 0.0
    var dep_Map:mutable.HashMap[Int,Double] = mutable.HashMap()
    val centers: mutable.HashMap[Int, mutable.HashMap[String,Double]] = mutable.HashMap[Int, mutable.HashMap[String,Double]]()
    var mode: Set[Int] = Set()
    var rhmats:RDD[(String,mutable.HashMap[Int,Int])] = null
    var data:RDD[(String,(mutable.HashMap[Int,Double],String))] = sc.textFile(classpath, partitionNum).map(x=>{
      val arr = x.split(",")
      val map:mutable.HashMap[Int,Double] = mutable.HashMap()
      (arr(0),(map,arr(1)))
    })
    data.take(1)
    
    var count = 1
    for (i <- 0 until AttrNum) {
      println("#####Calculating Feature-"+(i+1).toString+"#####")
      val t0 = System.currentTimeMillis()
      val filepath = path+(i+1).toString+"/part-00000"
      /** Cal single LU */
      println("   Calculating single LU...")
      val fileRdd = sc.textFile(filepath, partitionNum).persist(StorageLevel.MEMORY_AND_DISK)
      val LU = fileRdd.map(x=>{
          val arr = x.split(",")
        (arr(2).toInt,(arr(1).toDouble,arr(1).toDouble))
      }).reduceByKey((x1, x2) => (Math.min(x1._1, x2._1), Math.max(x1._2, x2._2))).collect()

      /** Cal single RHMat */
        println("   Calculatingsingle rhmat...")
      val rhmat1 = fileRdd.map(x=>{
          val arr = x.split(",")
          val f = arr(1).toDouble
          var col = 0
          for(i<-LU.indices){
            val ele = LU(i)
            val tup = LU(i)._2
            if (f>=tup._1 && f<=tup._2){
              col = col + (1 << ele._1)
            }
          }
        (arr(0),col)
      }).persist(StorageLevel.MEMORY_AND_DISK)
      /** Cal single Dep */
      println("   Calculatingsingle Depen...")
      val dep_single = rhmat1.map(x=>{
          val col = x._2
          val res = Math.log(col)/Math.log(2)
          var flag = 1.0
          if (res-res.round!=0.0){
            flag=0.0
          }
        flag
      }).reduce(_+_)/n
      
      if (dep_single>dep_mean){
        dep_Map+=(i->dep_single)
        //Merge rhmat and rhmats
        println("   Calculatingcombine rhmats...")
        if (mode.isEmpty){
          /** Combine rhmat with rhmats */
          val rhmats_new = rhmat1.map(x=>{
            val map:mutable.HashMap[Int,Int] = mutable.HashMap()
            map.put(i,x._2)
            (x._1,map)
          }).persist(StorageLevel.MEMORY_AND_DISK)
          rhmats_new.take(1)
          rhmats = rhmats_new
        }
        else{
          val rhmats_new = rhmats.join(rhmat1).map(x=>{
            val map = x._2._1
            map.put(i,x._2._2)
            (x._1,map)
          }).persist(StorageLevel.MEMORY_AND_DISK)
          rhmats_new.take(1)
          rhmats.unpersist()
          rhmats = rhmats_new
        }
        rhmat1.unpersist()

        //Calculating Dependency
        mode=mode.+(i)
        var dep_New = rhmats.map(x=>getDepen(x._2,mode)).reduce(_+_)
        dep_New = dep_New/n
        //When Significance bigger than 0
        if (dep_New>dep_Set){
          println("   Adding Feature"+(i+1).toString)
          dep_Set = dep_New
          dep_mean = dep_Set / mode.size

          // Calculating center pointer
          println("   Calculating centers...")
          val center = fileRdd.map(
            x => {
              val arr = x.split(",")
              val f = arr(1).toDouble
              val cls = arr(2)
              (cls,(f,1.0))
            }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._1/x._2._2)).collect().toMap
          val center_n:mutable.HashMap[String,Double] = mutable.HashMap()
          center_n ++= center
          centers.put(i,center_n)

          //Merge data
          /** Combine singleData with data */
          val fiRdd = fileRdd.map(
            x => {
              val arr = x.split(",")
              (arr(0),arr(1))
            })
          val data_new = data.join(fiRdd).map(
            x=>{
              val f = x._2._2.toDouble
              val cls = x._2._1._2
              val map = x._2._1._1
              map.put(i,f)
              (x._1,(map,cls))
            }
          ).persist(StorageLevel.MEMORY_AND_DISK)
          data_new.take(1)
          data.unpersist()
          fileRdd.unpersist()
          data = data_new
        }
        else if(dep_New==dep_Set) { //Significance is equal to 0
          println("   Redundancy Analysis...")
          val index_del = nonSignf(rhmats,mode,i,n,3)
          println(data.getNumPartitions)
          if (index_del.size<1){
            println("|B| is samller than 1, deleting featrue"+(i+1).toString)
            mode=mode.-(i)
            /**deleting matrix of redundancy featrues*/
            val rhmats_del = rhmats.map(x=>{
              val map = x._2
              map.remove(i)
              (x._1,map)
            }).persist(StorageLevel.MEMORY_AND_DISK)
            rhmats_del.take(1)
            rhmats.unpersist()
            rhmats = rhmats_del
            fileRdd.unpersist()
          }else{
            /** Combine singleData with data */
            val fiRdd = fileRdd.map(
              x => {
                val arr = x.split(",")
                (arr(0),arr(1))
              })
            val data_new = data.join(fiRdd).map(
              x=>{
                val f = x._2._2.toDouble
                val cls = x._2._1._2
                val map = x._2._1._1
                map.put(i,f)
                (x._1,(map,cls))
              }
            ).persist(StorageLevel.MEMORY_AND_DISK)
            data_new.take(1)
            data.unpersist()
            fileRdd.unpersist()
            data = data_new

            // Calculating center pointer
            println("   Calculating centers...")
            val center = fileRdd.map(
              x => {
                val arr = x.split(",")
                val f = arr(1).toDouble
                val cls = arr(2)
                (cls,(f,1.0))
              }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._1/x._2._2)).collect().toMap
            val center_n:mutable.HashMap[String,Double] = mutable.HashMap()
            center_n ++= center
            centers.put(i,center_n)
            fileRdd.unpersist()

            if (index_del.size>1){
              println("|B| is bigger than 1, deleting featrue"+index_del.toString()+" keep cuurrent feature "+(i+1).toString)
              /**de;eting data of redundancy features*/
              val data_del = data.map(x => {
                val map = x._2._1
                for(ind<-index_del){
                  map.remove(ind)
                }
                (x._1,(map, x._2._2))
              }).persist(StorageLevel.MEMORY_AND_DISK)
              data_del.take(1)
              data.unpersist()
              data=data_del

              /**deleting matrix of redundancy featrues*/
              val rhmats_del = rhmats.map(x=>{
                val map = x._2
                for(ind<-index_del){
                  map.remove(ind)
                }
                (x._1,map)
              }).persist(StorageLevel.MEMORY_AND_DISK)
              rhmats_del.take(1)
              rhmats.unpersist()
              rhmats = rhmats_del

              for(index<-index_del) {
                mode = mode.-(index)
                centers.remove(index)
                dep_Map.remove(index)
              }
            }
            else{
              println("|B| is equal to 1")
              println("   deleting a redundancy feature ...")
              var ind_del = 0
              val f_del = index_del.head
              if(dep_New<1){
                val info_i = info_BN(rhmats,data,mode,f_del,centers,n)
                val info_del = info_BN(rhmats,data,mode,i,centers,n)
                if (info_del>info_i) {
                  ind_del = i
                }
                else if (info_del<info_i) {
                  ind_del = f_del
                }
                else {
                  if (Random.nextDouble()>0.5){
                    ind_del = i
                  }
                  else {
                    ind_del = f_del
                  }
                }
              }

              if(dep_New==1.0){
                var mode1: Set[Int] = Set()
                mode1=mode1.+(i)
                mode1=mode1.+(f_del)
                val info_i:Double = info_BN(rhmats,data,mode1,f_del,centers,n)
                val info_del:Double = info_BN(rhmats,data,mode1,i,centers,n)
                val dep_i = dep_Map.getOrElse(i,0.0)
                val dep_del = dep_Map.getOrElse(f_del,0.0)
                if ((dep_del+info_del)>(dep_i+info_i)) {
                  ind_del = i
                }
                else if ((dep_del+info_del)<(dep_i+info_i)) {
                  ind_del = f_del
                }
                else {
                  if (Random.nextDouble()>0.5){
                    ind_del = i
                  }
                  else {
                    ind_del = f_del
                  }
                }
              }

              /**de;eting data of redundancy features*/
              val data_del = data.map(x => {
                val map = x._2._1
                map.remove(ind_del)
                (x._1,(map, x._2._2))
              }).persist(StorageLevel.MEMORY_AND_DISK)
              data_del.take(1)
              data.unpersist()
              data = data_del
              /**deleting matrix of redundancy featrues*/
              val rhmats_del = rhmats.map(x=>{
                val map = x._2
                map.remove(ind_del)
                (x._1,map)
              }).persist(StorageLevel.MEMORY_AND_DISK)
              rhmats_del.take(1)
              rhmats.unpersist()
              rhmats = rhmats_del

              mode = mode.-(ind_del)
              centers.remove(ind_del)
              dep_Map.remove(ind_del)
            }
          }

          dep_mean = dep_Set / mode.size
        }
      }
      else {
        fileRdd.unpersist()
        rhmat1.unpersist()
      }
    }
    
  }
}
