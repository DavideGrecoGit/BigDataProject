package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class QueryFormaterMap implements MapFunction<Row,Query> {

	private static final long serialVersionUID = 6475166483071609772L;

	private transient TextPreProcessor processor;
	
	@Override
	public Query call(Row value) throws Exception {
	
		if (processor==null) processor = new TextPreProcessor();
		
		String originalQuery = value.mkString();
		
		List<String> queryTerms = processor.process(originalQuery);
		
		short[] queryTermCounts = new short[queryTerms.size()];
		for (int i =0; i<queryTerms.size(); i++) queryTermCounts[i] = (short)1;
		
		Query query = new Query(originalQuery, queryTerms, queryTermCounts);
		
		return query;
	}

}
