package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class StringContentToNewsStatisticMap implements MapFunction<Tuple2<NewsArticle, String>, Tuple2<NewsArticle, NewsStatistic>> {

	private static final long serialVersionUID = -7006056215226895981L;
	
	private int currentDocCount = 0;
	
	@Override
	public Tuple2<NewsArticle, NewsStatistic> call(Tuple2<NewsArticle, String> value) throws Exception {
		String content = value._2;
		return new Tuple2<NewsArticle, NewsStatistic>(value._1, new NewsStatistic(process(content), currentDocCount));
	}
	
	
	private HashMap<String, Integer> process(String text) {
		String[] inputTokens = text.split(" ");
		HashMap<String, Integer> finalMap = new HashMap<String, Integer>();

		if (inputTokens==null) return finalMap;
		
		for (int i =0; i<inputTokens.length; i++) {
			if (inputTokens[i]==null) continue;
			currentDocCount++;
			if(finalMap.containsKey(inputTokens[i])) {
				finalMap.put(inputTokens[i], finalMap.get(inputTokens[i])+1);
			} else {
				finalMap.put(inputTokens[i], 1);
			}
		}
		
		return finalMap;
	}

}
