package src.uk.ac.gla.dcs.bigdata.apps;

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
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsToArticlesStatsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ReduceNewsStatistic;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoreMapping;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoresToId;
import uk.ac.gla.dcs.bigdata.studentfunctions.ScoresToResults;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

/**
 * This is the main class where the Spark topology is specified.
 * 
 * Running this class executes the topology defined in the rankDocuments()
 * method in local mode, although this may be overridden by the spark.master
 * environment variable.
 */
public class AssessedExercise {

	/**
	 * Main method that runs what is described above.
	 * 
	 * @author Richard
	 */
	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // Hadoop directory as a Java file, helps getting absolute path
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property for Spark

		// The code submitted for the assessed exercise may be run in either local or
		// remote modes. Configuration of this will be performed based on an environment
		// variable
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

	/**
	 * Method that given queries and articles, returns a list of 10 most relevant of
	 * these latter.
	 * 
	 * @author Davide, Manuel, Paul
	 */
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		// Get the start time (useful for the Evaluation section of the report)
		long startTime = System.currentTimeMillis();

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects

		// this converts each row into a Query
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class));
		// this converts each row into a NewsArticle
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class));

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// Corpus length counter
		LongAccumulator totalDocsInCorpus = spark.sparkContext().longAccumulator();

		// Get StringContent from NewsArticle
		NewsToArticlesStatsFlatMap stringContentFunction = new NewsToArticlesStatsFlatMap(totalDocsInCorpus);
		Encoder<Tuple2<NewsArticle, NewsStatistic>> newsEncoder = Encoders.tuple(Encoders.bean(NewsArticle.class),
				Encoders.bean(NewsStatistic.class));
		Dataset<Tuple2<NewsArticle, NewsStatistic>> articleStats = news.flatMap(stringContentFunction, newsEncoder);

		// Compute statistics of the entire corpus
		Tuple2<NewsArticle, NewsStatistic> corpusStats = articleStats.reduce(new ReduceNewsStatistic());
		Broadcast<NewsStatistic> broadcastCorpusStats = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(corpusStats._2());

		// Make the queries accessible to all nodes
		List<Query> serialisedQueries = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(serialisedQueries);

		// Compute scores of articles based on a given query
		ScoreMapping newsToScore = new ScoreMapping(broadcastCorpusStats, totalDocsInCorpus, broadcastQueries);
		Encoder<Tuple2<Query, RankedResult>> resultScoresEncoder = Encoders.tuple(Encoders.bean(Query.class),
				Encoders.bean(RankedResult.class));
		Dataset<Tuple2<Query, RankedResult>> resultScores = articleStats.flatMap(newsToScore, resultScoresEncoder);

		// Group results by query
		ScoresToId groupByQuery = new ScoresToId();
		KeyValueGroupedDataset<Query, Tuple2<Query, RankedResult>> results = resultScores.groupByKey(groupByQuery,
				Encoders.bean(Query.class));

		// Convert the results to DocumentRanking
		ScoresToResults scoresToResults = new ScoresToResults();
		Encoder<DocumentRanking> rankedResultsEncoder = Encoders.bean(DocumentRanking.class);
		Dataset<DocumentRanking> rankedResults = results.mapGroups(scoresToResults, rankedResultsEncoder);

		// Take the time again and report the runtime
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		System.out.println("Execution time: " + executionTime + " ms");

		// Return the List of DocumentRankings
		return rankedResults.collectAsList();
	}

}
