import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

public class Main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("App");
        JavaSparkContext context = new JavaSparkContext(conf);
        //JavaRDD<String> stringRDD = context.textFile("src/main/resources/hotels10.csv");

        SparkSession spark = SparkSession.builder().getOrCreate();

//        Dataset<Row> df = spark.read()

//        Dataset<Row> df = spark.read().csv("src/main/resources/hotels10.csv");

  //      df.show();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", "test")
                .load();

        df.show();
//        df.write().json("test.json");
    }

}