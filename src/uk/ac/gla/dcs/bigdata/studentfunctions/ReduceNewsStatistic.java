package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;

import org.apache.spark.api.java.function.ReduceFunction;

import scala.Function2;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

public class ReduceNewsStatistic implements ReduceFunction<Tuple2<NewsArticle, NewsStatistic>> {

	private static final long serialVersionUID = -3774580297290424421L;

	@Override
	public Tuple2<NewsArticle, NewsStatistic> call(Tuple2<NewsArticle, NewsStatistic> v1, Tuple2<NewsArticle, NewsStatistic> v2)
			throws Exception {
		HashMap<String, Integer> newMap = new HashMap<String, Integer>(v1._2.getTermFrequencyMap());
		v2._2.getTermFrequencyMap().forEach(
					(key, value) -> newMap.merge(key, value, (x1, x2) -> x1 + x2)
				);
		int combinedTotal = v1._2.getDocLength() + v2._2.getDocLength();
		return new Tuple2<NewsArticle, NewsStatistic>(v1._1, new NewsStatistic(newMap, combinedTotal));
	}

}
