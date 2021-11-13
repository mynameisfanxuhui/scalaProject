import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer;

// this is the code for Problem1
// @author: xli14@wpi.edu

object Problem2 {

  def main(args:Array[String]):Unit= {

    // file paths
    val pointFile = "file:///E:/放桌面/DS503/project3_2/DatasetP.txt"
    val outputPath1 = "file:///E:/放桌面/DS503/hw/q2"
    val outputPath2 = "file:///E:/放桌面/DS503/hw/q3"

    val conf = new SparkConf().setAppName("points")//.setMaster("local")
    val sc = new SparkContext(conf)
    val points = sc.textFile(pointFile)

    // count the points in one cell
    // output: ((cellindex), (number of points))
    val counts = points.map(line => {
      var x = line.split(",")(0).toFloat
      var y = line.split(",")(1).toFloat
      var xindex = Math.ceil(x / 20).toInt
      var yindex = Math.ceil(y / 20).toInt
      var cellindex = xindex + (yindex - 1) * 500
      ((cellindex),1)
    }).reduceByKey({(x, y) => x + y})

    // make the neighbor cells
    // output: ((neighborindex),(num of points, 1, cellindex))
    val cellleft = counts.map(t => (t._1 - 1, (t._2, 1, t._1)))
    val cellright = counts.map(t => (t._1 + 1, (t._2, 1, t._1)))
    val cellup = counts.map(t => (t._1 - 500, (t._2, 1, t._1)))
    val cellupleft = counts.map(t => (t._1 - 500 - 1, (t._2, 1, t._1)))
    val cellupright = counts.map(t => (t._1 - 500 + 1, (t._2, 1, t._1)))
    val celldown = counts.map(t => (t._1 + 500, (t._2, 1, t._1)))
    val celldownleft = counts.map(t => (t._1 + 500 - 1, (t._2, 1, t._1)))
    val celldownright = counts.map(t => (t._1 + 500 + 1, (t._2, 1, t._1)))

    // initialize a clean cell to prepare for the neighbor calculate
    val clean = counts.map(t => (t._1, (0, 0, "")))

    // get the neighbors of each cell
    // output:((cellindex),(sum of neighbor points, number of neighbors, ID list of neighbors))
    val neighbor = clean.leftOuterJoin(cellleft).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(cellright).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(cellup).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(cellupleft).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(cellupright).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(celldown).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(celldownleft).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    }).leftOuterJoin(celldownright).map(t => {
      var Npoints = 0
      var Nneighbor= 0
      var neighborID = ""
      if(t._2._2.isEmpty == true){
        Npoints = 0
        Nneighbor = 0
      }else{
        Npoints = t._2._2.get._1
        Nneighbor = t._2._2.get._2
        neighborID = "D" + t._2._2.get._3.toString
      }
      (t._1, ((t._2._1._1 + Npoints), (t._2._1._2 + Nneighbor), (t._2._1._3 + neighborID)))
    })

    // calculate the density
    // output: ((cellid), (density of this cell))
    val density = neighbor.join(counts).map{ t => (t._1, t._2._2.toFloat * t._2._1._2 / t._2._1._1)}


    // Step 2
    // output: Top50 id, Relative-Density Index
    val top50 = density.sortBy(-_._2).take(50)
    //top50.foreach(println)
    val top50RDD = sc.parallelize(top50)
    top50RDD.saveAsTextFile(outputPath1)

    // Step 3
    // output: neighbor id, neighbor Relative-Density Index, Top50 id

    val neighborRelation = neighbor.map(t => (t._1, t._2._3)).collect
    val neighborList = ArrayBuffer[(Int, Int)]()

    neighborRelation.foreach(line =>{
      var cellid = line._1
      var divide = line._2.split("D").foreach(neighborid => {
        if(neighborid != ""){
          var temp = (cellid, neighborid.toInt)
          neighborList += temp;
        }

      })
    })

    val neighborPair = neighborList.toArray
    val neighborRDD = sc.parallelize(neighborPair)

    val Top50neighbor = neighborRDD.join(top50RDD).map(t =>{ (t._1, t._2._1)})
    val Top50PairReverse = Top50neighbor.map(t => { (t._2, t._1) })
    val neighbor_density = Top50PairReverse.join(density).map { t => (t._1, t._2._2, t._2._1)}
    neighbor_density.saveAsTextFile(outputPath2)
  }
}