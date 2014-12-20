import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
 
public class Main {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));
    final int threshold = Integer.parseInt(args[1]);
 
    // split each document into words
    JavaRDD tokenized = sc.textFile(args[0]).flatMap(
      new FlatMapFunction() {
        public Iterable call(String s) {
          return Arrays.asList(s.split(" "));
        }
      }
    );
 
    // count the occurrence of each word
    JavaPairRDD counts = tokenized.mapToPair(
      new PairFunction() {
        public Tuple2 call(String s) {
          return new Tuple2(s, 1);
        }
      }
    ).reduceByKey(
      new Function2() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }
    );
 
    // filter out words with less than threshold occurrences
    JavaPairRDD filtered = counts.filter(
      new Function, Boolean>() {
        public Boolean call(Tuple2 tup) {
          return tup._2 >= threshold;
        }
      }
    );
 
    // count characters
    JavaPairRDD charCounts = filtered.flatMap(
      new FlatMapFunction, Character>() {
        public Iterable call(Tuple2 s) {
          ArrayList chars = new ArrayList(s._1.length());
          for (char c : s._1.toCharArray()) {
            chars.add(c);
          }
          return chars;
        }
      }
    ).mapToPair(
      new PairFunction() {
        public Tuple2 call(Character c) {
          return new Tuple2(c, 1);
        }
      }
    ).reduceByKey(
      new Function2() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }
    );
 
    System.out.println(charCounts.collect());
  }
}
