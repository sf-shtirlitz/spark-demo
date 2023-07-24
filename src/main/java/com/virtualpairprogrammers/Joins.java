package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class Joins {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
//        Logger.getLogger("org.apache").setLevel(Level.OFF);
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//
//        Logger rootLogger = Logger.getRootLogger();
//        rootLogger.setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        conf.log().atLevel(org.slf4j.event.Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitRaw = new ArrayList<>();
        visitRaw.add(new Tuple2<>(4,18));
        visitRaw.add(new Tuple2<>(6,4));
        visitRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1,"John"));
        usersRaw.add(new Tuple2<>(2,"Bob"));
        usersRaw.add(new Tuple2<>(3,"Alan"));
        usersRaw.add(new Tuple2<>(4,"Doris"));
        usersRaw.add(new Tuple2<>(5,"Marybelle"));
        usersRaw.add(new Tuple2<>(6,"Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        List<Tuple2<Integer, Tuple2<Integer, String>>> results = joinedRdd.take(Math.max(usersRaw.size(), visitRaw.size()));
        results.forEach(System.out::println);

        System.out.println();

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinedRdd = visits.leftOuterJoin(users);
        leftOuterJoinedRdd.collect().forEach(it -> System.out.println(it._1 + " : " + it._2._2.orElse("blank name").toUpperCase()));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinedRdd = visits.rightOuterJoin(users);
        rightOuterJoinedRdd.collect().forEach(it -> System.out.println("UserId: " + it._1 + " : Visits: " + it._2._1.orElse(0) + " : Name : " + it._2._2.toUpperCase()));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinedRdd = visits.fullOuterJoin(users);
        fullJoinedRdd.collect().forEach(it -> System.out.println("UserId: " + it._1 + " : Visits: " + it._2._1.orElse(0) + " : Name : " + it._2._2.orElse("no name").toUpperCase()));

//        joinedRdd.foreach(System.out::println);

/*        JavaRDD<String> initialRdd = sc.textFile("s3n://vpp-spark-demo-ak/input.txt");//("src/main/resources/subtitles/input.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);*/


        sc.close();
    }
}
