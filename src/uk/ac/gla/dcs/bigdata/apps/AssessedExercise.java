package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterAndConvertContent;
import uk.ac.gla.dcs.bigdata.studentfunctions.IdToContent;
import uk.ac.gla.dcs.bigdata.studentfunctions.IdToNews;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsToId;
import uk.ac.gla.dcs.bigdata.studentfunctions.ReduceNewsStatistic;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoreMapping;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoresToId;
import uk.ac.gla.dcs.bigdata.studentfunctions.StringContentToNewsStatisticMap;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get
														// an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark
																			// finds it

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef == null)
			sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/" + System.currentTimeMillis());
			if (!outDirectory.exists())
				outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts
																										// each row into
																										// a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this
																											// converts
																											// each row
																											// into a
																											// NewsArticle

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// Debug
		// long lenghtCorpus = news.count();
		// System.out.println("Lenght of the Corpus: "+lenghtCorpus);

		// Debug
		// for (int i =0; i<queries.first().getQueryTerms().size(); i++) {
		// System.out.println(queries.first().getQueryTerms().get(i));
		// System.out.println(queries.first().getQueryTermCounts()[i]);
		// }

		// 2a. Query aggragetedQueries = queries.reduce(new QueriesReducer());

		// 3. Reduce ContentItems to have only paragraph text and title by key

		// - 3.1 Group newsArticle by id
		NewsToId keyFunction = new NewsToId();
		KeyValueGroupedDataset<String, NewsArticle> newsById = news.groupByKey(keyFunction, Encoders.STRING());

		// - 3.2 Transform List<ContentItem> into String, keep grouping by id

		FilterAndConvertContent stringContentFunction = new FilterAndConvertContent();
		Encoder<Tuple2<String, String>> keyStringEncoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
		Dataset<Tuple2<String, String>> stringContentById = newsById.flatMapGroups(stringContentFunction,
				keyStringEncoder);

		Encoder<Tuple2<String, NewsStatistic>> newsEncoder = Encoders.tuple(Encoders.STRING(),
				Encoders.bean(NewsStatistic.class));
		Dataset<Tuple2<String, NewsStatistic>> newsStats = stringContentById
				.flatMap(new StringContentToNewsStatisticMap(), newsEncoder);

		// baseline metrics
		Tuple2<String, NewsStatistic> baselineMetrics = newsStats.reduce(new ReduceNewsStatistic());
		Broadcast<NewsStatistic> broadcastMetrics = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(baselineMetrics._2);
		Broadcast<Long> totalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(newsStats.count());

		List<Query> serialisedQueries = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(serialisedQueries);
		
		// newsStats (<String, NewsStatistic>) to resultScores (<Query, String, Double>)
		ScoreMapping newsToScore = new ScoreMapping(broadcastMetrics, totalDocsInCorpus, broadcastQueries);
		Encoder<Tuple3<Query, String, Double>> resultScoresEncoder = Encoders.tuple(Encoders.bean(Query.class), Encoders.STRING(), Encoders.DOUBLE());
		Dataset<Tuple3<Query, String, Double>> resultScores = newsStats.flatMap(newsToScore, resultScoresEncoder);
		
		System.out.println("======================= "+resultScores.count()+" =======================");
		
		List<DocumentRanking> finalList = new ArrayList<DocumentRanking>();
		// For each query in the dataset...
		
		// for (Query query : serialisedQueries) {
		// 	// Calculate DPH scores
		// 	Encoder<Tuple2<String, Double>> scoreResultEncoder = Encoders.tuple(Encoders.STRING(),
		// 			Encoders.DOUBLE());
		// 	Dataset<Tuple2<String, Double>> scoreResults = newsStats
		// 			.map(new ScoreMapping(broadcastMetrics, totalDocsInCorpus, query), scoreResultEncoder);
		// 	// Sort the dataset by score and build its iterator
		// 	Dataset<Tuple2<String, Double>> sortedScoreResults = scoreResults.orderBy(functions.desc("_2"));
		// 	Iterator<Tuple2<String, Double>> resultIterator = sortedScoreResults.toLocalIterator();
		// 	// Build a list of RankedResults which are non-similar
		// 	List<RankedResult> resultsList = new ArrayList<RankedResult>();
		// 	IdToNews idToNews = new IdToNews();
		// 	IdToContent idToContent = new IdToContent();

		// 	// Until the size of the list is not 10...
		// 	while (resultsList.size() <= 10) {
		// 		Tuple2<String, Double> currResult = resultIterator.next();
				
		// 		try {
		// 			// Get the current article object
		// 			NewsArticle currArticle = idToNews.getArticle(currResult._1, newsById);
		// 			// Get its content
		// 			String currContent;
				
		// 			currContent = idToContent.getContent(currResult._1, stringContentById);
				
		// 			// Create RankedResults object
		// 			RankedResult result = new RankedResult(currResult._1, currArticle, currResult._2);
	
		// 			// Base case: add first item to the list
		// 			if (resultsList.isEmpty()) {
		// 				resultsList.add(result);
		// 			} else {
		// 				// Otherwise check if element is similar to any of the present articles
		// 				Boolean toAdd = true;
		// 				for (RankedResult item : resultsList) {
		// 					// Get the currItem content and compute distance
		// 					String itemContent = idToContent.getContent(item.getDocid(), stringContentById);
		// 					if (TextDistanceCalculator.similarity(currContent, itemContent) >= 0.5) {
		// 						toAdd = false;
		// 					}
		// 				}
		// 				// If not similar to anything, add it
		// 				if (toAdd) {
		// 					resultsList.add(result);
		// 				}
		// 			}
		// 		} catch (Exception e) {
		// 			// TODO Auto-generated catch block
		// 			e.printStackTrace();
		// 		}
		// 	}
		// 	// Create DocumentRanking object
		// 	DocumentRanking docRanking = new DocumentRanking(query, resultsList);
		// 	// Add the this to the final list
		// 	finalList.add(docRanking);
		// }

		// return finalList;
		return null;
	}

}
