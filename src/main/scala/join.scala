import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel


object join {
  def main(args: Array[String]): Unit = {
    // start spark session
    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val file_1 = args(0)
    val file_2 = args(1)
    val column_name = args(2)
    var join_type: String = ""

    // join_type and number of arguments validation
    if(args.length == 3 ) {}
      else if(args.length == 4 ){
      join_type = args(3)
      val possible_join_types = List("inner", "left", "right") // possible join types
      if(!possible_join_types.contains(join_type)){
        println("Bad join type argument, type one of: inner, left or right")
        System.exit(1)  // quit with 1 because i want to know what type of problem its is, every failure exit have specific code
      }
    }else{
      println("Wrong number of arguments, it should be 3 or 4 if join type is specified")
      System.exit(2)  // 2 - wrong number of arguments

    }

    try {
      // read from csv file with header, persist(StorageLevel.MEMORY_AND_DISK_SER) is set for the case,
      // if there is too much data to fit in memory, a disk is used. Stores data in memory in an ordered form.
      // if we only used X it would increase the footprint but we saved CPU time

      val df = spark.read.format("csv").option("header", "true").load(file_1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val df_2 = spark.read.format("csv").option("header", "true").load(file_2).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // check whether  column with column_name existing in columns in ours file
    if(!(parseColumnName(df,column_name) && parseColumnName(df_2,column_name) )){
      println("Such a column does not exist in one of the specified files")
      System.exit(3)  // 3 - wrong column name
    }

      // join files(df)
    var result  = spark.emptyDataFrame
    if(join_type !="") {
      result= df.join(df_2, df(column_name) === df_2(column_name), join_type).drop(df_2(column_name))
      // drop for not printing two times same column
    }
    else{
      // default join when join_type is not specified
      result=df.join(df_2, df(column_name) === df_2(column_name)).drop(df_2(column_name))
    }
      // print join result to standard output
      result.collect.foreach(println)
    }catch{
          // when one of files does not exist
      case e : Exception =>  println("One of the specified files does not exist"); System.exit(4)
    }
    }
    def parseColumnName(df : DataFrame, column_name : String): Boolean ={
      df.columns.contains(column_name)
    }
}
