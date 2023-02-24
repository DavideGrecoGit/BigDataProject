package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;

import scala.Function1;
import scala.Tuple2;

/**
 * Extracts some article content given its ID.
 * 
 * @autor Manuel
 */

public class IdToContent {
	public IdToContent() {

	}

	public String getContent(String docId, Dataset<Tuple2<String, String>> dataset) throws Exception {
		// Filter the dataset to return a matching article content given its id
		Dataset<String> filteredValues = dataset
				.filter((Function1<Tuple2<String, String>, Object>) tuple -> tuple._1().equals(docId))
				.map((MapFunction<Tuple2<String, String>, String>) tuple -> tuple._2(), Encoders.STRING());

		List<String> valueList = filteredValues.collectAsList();

		if (valueList.isEmpty()) {
			// Handle the case where no value matches the target key
			return null;
		} else {
			// Return the first value that matches the target key
			return valueList.get(0);
		}
	}

}
