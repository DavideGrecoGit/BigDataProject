package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultsList;

public class ScoresToResults implements MapGroupsFunction<Query, Tuple3<Query, String, Double>, DocumentRanking>{

    @Override
    public DocumentRanking call(Query key, Iterator<Tuple3<Query, String, Double>> values)
            throws Exception {
        
        List<Tuple3<Query, String, Double>> valuesList = IteratorUtils.toList(values);

        Comparator<Tuple3<Query, String, Double>> comparator = new Comparator<Tuple3<Query, String, Double>>(){

            public int compare(Tuple3<Query, String, Double> tupleA,
                    Tuple3<Query, String, Double> tupleB)
            {
                return tupleA._3().compareTo(tupleB._3());
            }

        };

        Collections.sort(valuesList, comparator);
        Iterator<Tuple3<Query, String, Double>> resultIterator = valuesList.iterator();

        // Build a list of RankedResults which are non-similar
        List<RankedResult> resultsList = new ArrayList<RankedResult>();

        // Until the size of the list is not 10...
        while (resultsList.size() <= 10) {
            Tuple3<Query, String, Double> currResult = resultIterator.next();
        
            
            // Create RankedResults object
            RankedResult result = new RankedResult(currResult._2(), null, currResult._3());

            // Base case: add first item to the list
            if (resultsList.isEmpty()) {
                resultsList.add(result);
            } else {
                // // Otherwise check if element is similar to any of the present articles
                // Boolean toAdd = true;
                // for (RankedResult item : resultsList) {
                //     // Get the currItem content and compute distance
                //     if (TextDistanceCalculator.similarity(currResult._2(), item.getArticle().getTitle()) >= 0.5) {
                //         toAdd = false;
                //     }
                // }
                // // If not similar to anything, add it
                // if (toAdd) {
                //     resultsList.add(result);
                // }

                resultsList.add(result);
            }
        }

        return new DocumentRanking(key, resultsList);
    }
    
}
