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

//	@Override
//	public Iterator<SteamGameStats> call(String key, Iterator<SteamGameStats> games) throws Exception {
//		
//		List<SteamGameStats> filteredGamesByPrice = new ArrayList<SteamGameStats>();
//		
//		// get the list of words
//		Set<String> stopwords = broadcastStopwords.value();
//		
//		boolean supportsMac = key.contains("Mac");
//		
//		while (games.hasNext()) {
//			SteamGameStats game = games.next();
//			
//			if (game.getPriceinitial()>=minPrice) {
//				
//				filteredGamesByPrice.add(game);
//				
//				// now lets calculate the extra statistics we wanted
//				String description = game.getDetaileddescrip();
//				if (description!=null) {
//					
//					docCountAccumulator.add(1);
//					
//					for (String word : description.split(" ")) { // split string on space character
//						if (!stopwords.contains(word.toLowerCase())) { // if word is not a stopword
//							
//							if (supportsMac) wordCountAccumulator.add(1);
//							
//						}
//					}
//					
//				}
//				
//			}
//			 
//		}
//		
//		return filteredGamesByPrice.iterator();
//	}
//
//
