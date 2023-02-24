package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Extracts the Id of a NewsArticle
 * @author davide
 *
 */

public class NewsToId implements MapFunction<NewsArticle, String>{

	private static final long serialVersionUID = 4942915316114333055L;

	@Override
	public String call(NewsArticle value) throws Exception {
		return value.getId();
	}

}

