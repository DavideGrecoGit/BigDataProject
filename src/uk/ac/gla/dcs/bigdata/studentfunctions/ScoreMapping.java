package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class ScoreMapping implements MapFunction<Tuple2<String, NewsStatistic>, Tuple2<String, Double>> {

	private static final long serialVersionUID = 8738159622158851426L;
	
	private NewsStatistic baseMetrics;
	private long totalDocsInCorpus;
	
	public ScoreMapping(Broadcast<NewsStatistic> broadcastMetrics, Broadcast<Long> totalDocsInCorpus) {
		this.baseMetrics = broadcastMetrics.getValue();
		this.totalDocsInCorpus = totalDocsInCorpus.getValue();
	}

	@Override
	public Tuple2<String, Double> call(Tuple2<String, NewsStatistic> value) throws Exception {
		double totalScore = 0;
		for(String word : value._2.getTermFrequencyMap().keySet()) {
			totalScore += DPHScorer.getDPHScore(baseMetrics.getTermFrequencyMap().get(word).shortValue(), 
					value._2.getTermFrequencyMap().get(word), 
					value._2.getDocLength(), 
					baseMetrics.getDocLength(),
					totalDocsInCorpus);
		}
		return new Tuple2<String, Double>(value._1, totalScore);
	}

}
