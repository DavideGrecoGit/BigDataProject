package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterAndConvertContent;
import uk.ac.gla.dcs.bigdata.studentfunctions.ReduceNewsStatistic;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoreMapping;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoresToId;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoresToResults;
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

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		
		// this converts each row into a Query
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); 
		// this converts each row into a NewsArticle
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); 

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// Debug
		// long lenghtCorpus = news.count();
		// System.out.println("--------- Corpus: "+lenghtCorpus);

		// Debug
		// long lenghtQueries = queries.count();
		// System.out.println("--------- Queries: "+lenghtQueries);

		// Get StringContent from NewsArticle 
		FilterAndConvertContent stringContentFunction = new FilterAndConvertContent();
		Encoder<Tuple2<NewsArticle, String>> newsArticlesEncoder = Encoders.tuple(Encoders.bean(NewsArticle.class), Encoders.STRING());
		Dataset<Tuple2<NewsArticle, String>> stringContentByArticle = news.flatMap(stringContentFunction, newsArticlesEncoder);
		
		// System.out.println("------ StringContent Articles "+stringContentByArticle.count()+" ------");

		// Calculate Statistics of each NewsArticle
		Encoder<Tuple2<NewsArticle, NewsStatistic>> newsEncoder = Encoders.tuple(Encoders.bean(NewsArticle.class), Encoders.bean(NewsStatistic.class));
		Dataset<Tuple2<NewsArticle, NewsStatistic>> newsStats = stringContentByArticle.map(new StringContentToNewsStatisticMap(), newsEncoder);
		
		System.out.println("======================= Stats Articles"+newsStats.count()+" =======================");

		// baseline metrics
		Tuple2<NewsArticle, NewsStatistic> baselineMetrics = newsStats.reduce(new ReduceNewsStatistic());
		Broadcast<NewsStatistic> broadcastMetrics = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(baselineMetrics._2);
		Broadcast<Long> totalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(newsStats.count());

		List<Query> serialisedQueries = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(serialisedQueries);
		
		// newsStats (<String, NewsStatistic>) to resultScores (<Query, String, Double>)
		ScoreMapping newsToScore = new ScoreMapping(broadcastMetrics, totalDocsInCorpus, broadcastQueries);
		Encoder<Tuple2<Query, RankedResult>> resultScoresEncoder = Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(RankedResult.class));
		Dataset<Tuple2<Query, RankedResult>> resultScores = newsStats.flatMap(newsToScore, resultScoresEncoder);
		
		System.out.println("======================= "+resultScores.count()+" =======================");

		ScoresToId groupByQuery = new ScoresToId();
		KeyValueGroupedDataset <Query, Tuple2<Query, RankedResult>> results = resultScores.groupByKey(groupByQuery, Encoders.bean(Query.class)); 
		
		System.out.println("======================= "+results.count().count()+" =======================");
		
		ScoresToResults scoresToResults = new ScoresToResults();
		Encoder<DocumentRanking> rankedResultsEncoder = Encoders.bean(DocumentRanking.class);
		Dataset<DocumentRanking> rankedResults = results.mapGroups(scoresToResults, rankedResultsEncoder);

		System.out.println("======================= Ranked Results count "+rankedResults.count()+" =======================");

		return rankedResults.collectAsList();
	}

}
