package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

public class NewsToArticlesStatsFlatMap implements FlatMapFunction<NewsArticle, Tuple2<NewsArticle, NewsStatistic>>{

	private static final long serialVersionUID = 6882302572907096250L;
	
	LongAccumulator totalDocsInCorpus;

	public NewsToArticlesStatsFlatMap(LongAccumulator totalDocsInCorpus){
		this.totalDocsInCorpus = totalDocsInCorpus;
	}

	@Override
	public Iterator<Tuple2<NewsArticle, NewsStatistic>> call(NewsArticle article) throws Exception {
		List<Tuple2<NewsArticle, NewsStatistic>> filteredStringContent = new ArrayList<Tuple2<NewsArticle, NewsStatistic>>();

		if(isArticleValid(article)){

			// Count only valid articles
			totalDocsInCorpus.add(1);
			
			NewsStatistic stats = new NewsStatistic(new HashMap<String, Integer>(), 0);
			int i = 0;

			// Iterates over all contents or stops when the title and 5 paragraphs have been processed
			// Title and paragraphs are checked separately in case title is split into two or more content items
			// or other exceptions

			for (Iterator<ContentItem> iter = article.getContents().iterator(); iter.hasNext() && i<6; ){
				ContentItem content = iter.next();

				if(content.getType().equals("title")){
					process(content.getContent(), stats);
				}
				
				if (content.getSubtype()!=null && content.getSubtype().equals("paragraph")) {
					process(content.getContent(), stats);
					i = i+1;
				}
			}

			if (stats.isEmpty()==false){
				filteredStringContent.add(new Tuple2<NewsArticle, NewsStatistic>(article,stats));
				// Debug
				// System.out.println(stats.getTermFrequencyMap().toString());
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
	

	public Void process(String text, NewsStatistic stats) {
		BaseTermPipelineAccessor termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords","PorterStemmer");
		Tokeniser tokeniser = Tokeniser.getTokeniser();
		String[] inputTokens = tokeniser.getTokens(text);
		
		if (inputTokens==null) return null;
		
		HashMap<String, Integer> finalMap = stats.getTermFrequencyMap();

		for (int i =0; i<inputTokens.length; i++) {
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);
			
			if (processedTerm==null) continue;
			
			stats.incrementDocLenght();

			if(finalMap.containsKey(inputTokens[i])) {
				finalMap.put(inputTokens[i], finalMap.get(inputTokens[i])+1);
			} else {
				finalMap.put(inputTokens[i], 1);
			}
		}
		
		stats.setTermFrequencyMap(finalMap);
		return null;
	}
}