package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */
public class NewsFormaterMap implements MapFunction<Row,NewsArticle> {

	private static final long serialVersionUID = -4631167868446468097L;

	private transient ObjectMapper jsonMapper;
	
	@Override
	public NewsArticle call(Row value) throws Exception {

		if (jsonMapper==null) jsonMapper = new ObjectMapper();
		
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);
		
		return article;
	}
		
		
	
}
