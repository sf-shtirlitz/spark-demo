package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.Optional;
import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "C:\\Work\\hadoop-3.3.6");
//		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!
		JavaPairRDD<Integer,Integer> chapterCountRdd = chapterData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
						.reduceByKey((value1, value2) -> value1 + value2);

		chapterCountRdd.collect().forEach(System.out::println);

		System.out.println();

		//Step 1 - remove any duplicated views
		//JavaPairRDD<Integer, Integer> viewDataUnique
		viewData = viewData.distinct();
		viewData.collect().forEach(System.out::println);

		//Step 2 get the course Ids into the RDD
		System.out.println();
		System.out.println("Flipping chapterId and userId from above output");
		JavaPairRDD<Integer,Integer> chapterUserRdd = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));
		chapterUserRdd.collect().forEach(System.out::println);

		System.out.println();

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterIdUserIdCourseId = chapterUserRdd.join(chapterData);
		chapterIdUserIdCourseId.collect().forEach(it -> System.out.println(it._1 + " : " + it._2._1 + " : " + it._2._2));

		System.out.println();

		//Step 3 - don't need chapter ids anymore, so reducing it
		JavaPairRDD<Tuple2<Integer, Integer>, Long> userIdCourseIdCount =  chapterIdUserIdCourseId.mapToPair(row -> {
					Integer userId = row._2._1;
					Integer courseId = row._2._2;
					return new Tuple2<Tuple2<Integer, Integer>, Long>(new Tuple2<Integer, Integer>(userId,courseId),1L);
				}
		);
		userIdCourseIdCount.collect().forEach(System.out::println);

		System.out.println();

		userIdCourseIdCount = userIdCourseIdCount.reduceByKey((value1, value2) -> value1 + value2);
 		userIdCourseIdCount.collect().forEach(System.out::println);
		System.out.println();

		//Step - 5 remove userIds
		JavaPairRDD<Integer, Long> courseIdCount = userIdCourseIdCount.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));
		courseIdCount.collect().forEach(System.out::println);


		System.out.println();

		//Step 6 - add in the total chapter count
		JavaPairRDD<Integer, Tuple2<Long, Integer>> courseIdViewsOf = courseIdCount.join(chapterCountRdd);
		courseIdViewsOf.collect().forEach(System.out::println);
		System.out.println();

		//Step 7 - convert to percentage
		JavaPairRDD<Integer, Double> courseIdPercentage = courseIdViewsOf.mapValues(value -> (double)value._1/value._2);
		courseIdPercentage.collect().forEach(System.out::println);
		System.out.println();

		//Step 8 - convert to scores
		JavaPairRDD<Integer, Long> courseIdPoints = courseIdPercentage.mapValues(value -> {
			if(value > .9) return 10L;
			if(value > .5) return 4L;
			if(value > .25) return 2L;
			return 0L;
		});
		courseIdPoints.collect().forEach(System.out::println);
		System.out.println();

		//Step 9
		JavaPairRDD<Integer, Long> courseRating = courseIdPoints.reduceByKey((value1, value2) -> value1 + value2);
		courseRating.collect().forEach(System.out::println);
		System.out.println();

		//Step 10 - join with titles
		JavaPairRDD<Integer, Tuple2<Long,String>> courseNameRating = courseRating.join(titlesData);
		courseNameRating.collect().forEach(System.out::println);

		System.out.println();
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(Integer.valueOf(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(Integer.valueOf(columns[0]), Integer.valueOf(columns[1]));
				     });
	}
}
