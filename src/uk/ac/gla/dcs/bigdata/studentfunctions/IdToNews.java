package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Extracts a NewsArticle given its ID.
 * 
 * @autor Manuel
 */

public class IdToNews {
	public IdToNews() {

	}

	public NewsArticle getArticle(String docId, KeyValueGroupedDataset<String, NewsArticle> keyValueDs)
			throws Exception {
		// Filter the keyvalue dataset to return a matching NewsArticle given its id
		Dataset<NewsArticle> articlesWithTargetValue = keyValueDs
				.flatMapGroups((FlatMapGroupsFunction<String, NewsArticle, NewsArticle>) (key, iterator) -> {
					if (key.equals(docId)) {
						List<NewsArticle> articles = new ArrayList<>();
						while (iterator.hasNext()) {
							articles.add(iterator.next());
						}
						return articles.iterator();
					} else {
						return Collections.emptyIterator();
					}
				}, Encoders.bean(NewsArticle.class));

		List<NewsArticle> articleList = articlesWithTargetValue.collectAsList();

		if (articleList.isEmpty()) {
			// Handle the case where no NewsArticle objects match the target value
			return null;
		} else {
			// Return the first NewsArticle object that matches the target value
			return articleList.get(0);
		}
	}

}
