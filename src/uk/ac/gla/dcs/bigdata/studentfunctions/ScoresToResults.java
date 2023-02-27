package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/**
 * Class that takes a class and an iterator of results and returns a
 * DocumentRankingObjects.
 * 
 * @author Davide, Manuel, Paul
 */
public class ScoresToResults implements MapGroupsFunction<Query, Tuple2<Query, RankedResult>, DocumentRanking> {

	private static final long serialVersionUID = -4375038578249487281L;

	@Override
	public DocumentRanking call(Query key, Iterator<Tuple2<Query, RankedResult>> values) throws Exception {
		// Flatten iterator to list
		List<Tuple2<Query, RankedResult>> valuesList = IteratorUtils.toList(values);

		// Create a comparator to sort the results based on their score
		Comparator<Tuple2<Query, RankedResult>> comparator = new Comparator<Tuple2<Query, RankedResult>>() {
			@Override
			public int compare(Tuple2<Query, RankedResult> arg0, Tuple2<Query, RankedResult> arg1) {
				return Double.compare(arg1._2().getScore(), arg0._2().getScore());
			}
		};
		Collections.sort(valuesList, comparator);
		Iterator<Tuple2<Query, RankedResult>> resultIterator = valuesList.iterator();

		// Build a list of RankedResults which are non-similar
		List<RankedResult> resultsList = new ArrayList<RankedResult>();

		// Until the size of the list is not 10...
		while (resultsList.size() <= 10 && resultIterator.hasNext()) {
			RankedResult currResult = resultIterator.next()._2();

			// Base case: add first item to the list
			if (currResult.getArticle().getTitle() != null && !currResult.getArticle().getTitle().isEmpty()) {
				if (resultsList.isEmpty()) {
					resultsList.add(currResult);
				} else {
					// Otherwise check if element is similar to any of the present articles
					Boolean toAdd = true;

					for (RankedResult item : resultsList) {
						// Get the currItem content and compute distance
						if (TextDistanceCalculator.similarity(currResult.getArticle().getTitle(),
								item.getArticle().getTitle()) < 0.5) {
							toAdd = false;
							break;
						}
					}
					// If not similar to anything, add it
					if (toAdd) {
						resultsList.add(currResult);
					}
				}
			}
		}

		return new DocumentRanking(key, resultsList);
	}

}
