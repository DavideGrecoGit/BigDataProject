package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryReducer implements ReduceFunction<Query> {
	
	private static final long serialVersionUID = 5791941830936823025L;

	@Override
	public Query call(Query v1, Query v2) throws Exception {
		HashSet<String> terms = new HashSet<String>();
		v1.getQueryTerms().forEach((term) -> terms.add(term));
		v2.getQueryTerms().forEach((term) -> terms.add(term));
		List<String> newList = new ArrayList<String>(terms);
		return new Query(null, newList, null);
	}

}
