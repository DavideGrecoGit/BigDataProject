package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class StringContentToNewsStatisticMap implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, NewsStatistic>> {

	private static final long serialVersionUID = -7006056215226895981L;
	
	private int currentDocCount = 0;
	
	@Override
	public Iterator<Tuple2<String, NewsStatistic>> call(Tuple2<String, String> value) throws Exception {
		String content = value._2;
		ArrayList<Tuple2<String, NewsStatistic>> result = new ArrayList<Tuple2<String, NewsStatistic>>();
		result.add(new Tuple2<String, NewsStatistic>(value._1, new NewsStatistic(process(content), currentDocCount)));
		currentDocCount = 0;
		return result.iterator();
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
