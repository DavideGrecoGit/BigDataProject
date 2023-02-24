package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterAndConvertContent implements FlatMapGroupsFunction<String, NewsArticle, Tuple2<String, String>>{

	private static final long serialVersionUID = 6882302572907096250L;
	
	@Override
	public Iterator<Tuple2<String, String>> call(String key, Iterator<NewsArticle> values) throws Exception {
		TextPreProcessor textProcessor = new TextPreProcessor();
		List<Tuple2<String, String>> filteredStringContent = new ArrayList<Tuple2<String, String>>();
		
		while(values.hasNext()) {
			
			NewsArticle article = values.next();
			
			StringBuilder contentBuilder = new StringBuilder();
			
			for (ContentItem content : article.getContents()) {
				
				
				if(content.getType().equals("title") || (content.getSubtype()!=null && content.getSubtype().equals("paragraph"))) {
					contentBuilder.append(" ");  // make sure there's spacing between all words
					contentBuilder.append(textProcessor.process(content.getContent()));
				}
			}
			
			if(contentBuilder.length()!=0) {
				filteredStringContent.add(new Tuple2<String,String>(key,contentBuilder.toString()));
			}
		}
	 	
		return filteredStringContent.iterator();
	}
}