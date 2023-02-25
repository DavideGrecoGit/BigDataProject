package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterAndConvertContent implements FlatMapFunction<NewsArticle, Tuple2<NewsArticle, String>>{

	private static final long serialVersionUID = 6882302572907096250L;
	
	@Override
	public Iterator<Tuple2<NewsArticle, String>> call(NewsArticle article) throws Exception {
		TextPreProcessor textProcessor = new TextPreProcessor();
		StringBuilder contentBuilder = new StringBuilder();
		List<Tuple2<NewsArticle, String>> filteredStringContent = new ArrayList<Tuple2<NewsArticle, String>>();
		
		if(isArticleValid(article)){
			
			int i = 0;
			
			// Iterates over all contents or stops when title and 5 paragraphs have been processed
			// Title and paragraphs are checked separately in case title is split into two or more content items
			// or other exceptions

			for (Iterator<ContentItem> iter = article.getContents().iterator(); iter.hasNext() && i<6; ){
				ContentItem content = iter.next();

				if(content.getType().equals("title")){
					contentBuilder.append(" ");  // make sure there's spacing between all words
					contentBuilder.append(textProcessor.process(content.getContent()));
				}
				
				if (content.getSubtype()!=null && content.getSubtype().equals("paragraph")) {
					contentBuilder.append(" ");  // make sure there's spacing between all words
					contentBuilder.append(textProcessor.process(content.getContent()));
					i = i+1;
				}
			}
			
			if(contentBuilder.length()!=0) {
				filteredStringContent.add(new Tuple2<NewsArticle,String>(article ,contentBuilder.toString()));
			}
		}

		return filteredStringContent.iterator();
	}

	public Boolean isArticleValid(NewsArticle article){
		if(article == null){
			return false;
		}

		if(article.getId() == null || article.getId().isEmpty()){
			return false;
		}
		
		if(article.getTitle() == null || article.getTitle().isEmpty()){
			return false;
		}

		return true;
	}
}