package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class ScoreMapping implements MapFunction<Tuple2<String, NewsStatistic>, Tuple2<String, Double>> {

	private static final long serialVersionUID = 8738159622158851426L;
	
	private NewsStatistic baseMetrics;
	private long totalDocsInCorpus;
	private Query query;
	
	public ScoreMapping(Broadcast<NewsStatistic> broadcastMetrics, Broadcast<Long> totalDocsInCorpus, Query query) {
		this.baseMetrics = broadcastMetrics.getValue();
		this.totalDocsInCorpus = totalDocsInCorpus.getValue();
		this.query = query;
	}

	@Override
	public Tuple2<String, Double> call(Tuple2<String, NewsStatistic> value) throws Exception {
		double totalScore = 0;
		for(int i = 0; i < query.getQueryTerms().size(); i++) { // should be query
			String word = query.getQueryTerms().get(i);
			totalScore += DPHScorer.getDPHScore(
					value._2.getTermFrequencyMap().get(word).shortValue(),
					baseMetrics.getTermFrequencyMap().get(word), 
					value._2.getDocLength(),
					baseMetrics.getDocLength() / totalDocsInCorpus,
					totalDocsInCorpus);
		}
		return new Tuple2<String, Double>(value._1, totalScore);
	}

}
