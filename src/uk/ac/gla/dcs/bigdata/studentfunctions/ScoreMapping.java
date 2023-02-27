package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

/**
 * Class that calculates the DPH score given a query and article content.s
 * 
 * @author Davide, Manuel, Paul
 */
public class ScoreMapping implements FlatMapFunction<Tuple2<NewsArticle, NewsStatistic>, Tuple2<Query, RankedResult>> {

	private static final long serialVersionUID = 8738159622158851426L;

	private NewsStatistic baseMetrics;
	private long totalDocsInCorpus;
	private List<Query> queries;

	public ScoreMapping(Broadcast<NewsStatistic> broadcastMetrics, LongAccumulator totalDocsInCorpus,
			Broadcast<List<Query>> queries) {
		this.baseMetrics = broadcastMetrics.getValue();
		this.totalDocsInCorpus = totalDocsInCorpus.value();
		this.queries = queries.getValue();
	}

	/**
	 * Method that calculates the DPH score.
	 */
	@Override
	public Iterator<Tuple2<Query, RankedResult>> call(Tuple2<NewsArticle, NewsStatistic> value) throws Exception {
		List<Tuple2<Query, RankedResult>> resultsList = new ArrayList<Tuple2<Query, RankedResult>>();

		// For each query...
		for (Query query : this.queries) {
			if (value._1() != null) {
				double totalScore = 0;
				// For each word in the query..
				for (String word : query.getQueryTerms()) {
					// If the word is in the article content, calculate DPH score
					if (value._2.getTermFrequencyMap().containsKey(word)) {
						totalScore += DPHScorer.getDPHScore(value._2.getTermFrequencyMap().get(word).shortValue(),
								baseMetrics.getTermFrequencyMap().get(word), value._2.getDocLength(),
								baseMetrics.getDocLength() / totalDocsInCorpus, totalDocsInCorpus);
					}
				}
				// Create a RankedResults object and add it to corresponding query list
				resultsList.add(new Tuple2<Query, RankedResult>(query,
						new RankedResult(value._1().getId(), value._1(), totalScore)));
			}
		}
		return resultsList.iterator();
	}
}
