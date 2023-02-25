package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class ScoreMapping implements FlatMapFunction<Tuple2<String, NewsStatistic>, Tuple3<Query, String, Double>>{
    
    private static final long serialVersionUID = 8738159622158851426L;
	
	private NewsStatistic baseMetrics;
	private long totalDocsInCorpus;
	private List<Query> queries;

    public ScoreMapping(Broadcast<NewsStatistic> broadcastMetrics, Broadcast<Long> totalDocsInCorpus, Broadcast<List<Query>>  queries) {
		this.baseMetrics = broadcastMetrics.getValue();
		this.totalDocsInCorpus = totalDocsInCorpus.getValue();
		this.queries = queries.getValue();
	}

    /**
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<Tuple3<Query, String, Double>> call(Tuple2<String, NewsStatistic> value) throws Exception {
        Double max = 100.0;
        Double min = 0.0;

        List<Tuple3<Query, String, Double>> resultsList = new ArrayList<Tuple3<Query, String, Double>>();

        for(Query query : this.queries){

            // Using dummy score since the following gives NullPointerException
            
            // double totalScore = 0;

            // for (String word : query.getQueryTerms()){
            //     totalScore += DPHScorer.getDPHScore(
            //         value._2.getTermFrequencyMap().get(word).shortValue(),
            //         baseMetrics.getTermFrequencyMap().get(word), 
            //         value._2.getDocLength(),
            //         baseMetrics.getDocLength() / totalDocsInCorpus,
            //         totalDocsInCorpus);
            // }

            Double totalScore = Math.floor(Math.random() * (max - min + 1) + min);

            resultsList.add(new Tuple3<Query, String, Double>(query, value._1(), totalScore));
        }
        
        return resultsList.iterator();
    }
}
