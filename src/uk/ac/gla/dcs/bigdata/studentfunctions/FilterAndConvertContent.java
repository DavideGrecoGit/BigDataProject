package uk.ac.gla.dcs.bigdata.studentfunctions;
import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterAndConvertContent implements MapFunction<NewsArticle, Tuple2<NewsArticle, String>>{

	private static final long serialVersionUID = 6882302572907096250L;
	
	@Override
	public Tuple2<NewsArticle, String> call(NewsArticle values) throws Exception {
		TextPreProcessor textProcessor = new TextPreProcessor();
			
		StringBuilder contentBuilder = new StringBuilder();
		
		for (ContentItem content : values.getContents()) {
			
			
			if(content.getType().equals("title") || (content.getSubtype()!=null && content.getSubtype().equals("paragraph"))) {
				contentBuilder.append(" ");  // make sure there's spacing between all words
				contentBuilder.append(textProcessor.process(content.getContent()));
			}
		}
		
		if(contentBuilder.length()!=0) {
			return new Tuple2<NewsArticle,String>(values ,contentBuilder.toString());
		}
	 	
		return new Tuple2<NewsArticle,String>(values, "");
	}
}