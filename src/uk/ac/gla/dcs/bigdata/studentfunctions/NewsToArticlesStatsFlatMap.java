package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsStatistic;

/**
 * Class that extracts statistics of an article to then be fed into the DPH
 * scorer.
 * 
 * @author Davide, Manuel, Paul
 */

public class NewsToArticlesStatsFlatMap implements FlatMapFunction<NewsArticle, Tuple2<NewsArticle, NewsStatistic>> {

	private static final long serialVersionUID = 6882302572907096250L;

	LongAccumulator totalDocsInCorpus;
	HashSet<String> allQueryTerms;

	public NewsToArticlesStatsFlatMap(LongAccumulator totalDocsInCorpus, Broadcast<Query> broadcastAllQueryTerms) {
		this.totalDocsInCorpus = totalDocsInCorpus;
		this.allQueryTerms = new HashSet<String>(broadcastAllQueryTerms.getValue().getQueryTerms());
	}

	@Override
	public Iterator<Tuple2<NewsArticle, NewsStatistic>> call(NewsArticle article) throws Exception {
		List<Tuple2<NewsArticle, NewsStatistic>> filteredStringContent = new ArrayList<Tuple2<NewsArticle, NewsStatistic>>();

		// Check if the title and content are not empty
		if (isArticleValid(article)) {
			// Count only valid articles
			totalDocsInCorpus.add(1);

			NewsStatistic stats = new NewsStatistic(new HashMap<String, Integer>(), 0);
			// For each article...
			int i = 0;
			for (Iterator<ContentItem> iter = article.getContents().iterator(); iter.hasNext() && i < 6;) {
				// Take the first 5 paragraphs and the title,
				// remove stopwords and tokenize
				ContentItem content = iter.next();
				if (content != null) {					
					if (content.getType().equals("title")) {
						process(content.getContent(), stats);
					}
					if (content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
						process(content.getContent(), stats);
						i = i + 1;
					}
				}
			}

			// Consider only if content was present in the article
			if (!stats.isEmpty()) {
				filteredStringContent.add(new Tuple2<NewsArticle, NewsStatistic>(article, stats));
			}
		}
		return filteredStringContent.iterator();
	}

	/**
	 * Helper method that checks if an article is not null and has both title and
	 * content.
	 */
	public Boolean isArticleValid(NewsArticle article) {
		if (article == null) {
			return false;
		}

		if (article.getId() == null || article.getId().isEmpty()) {
			return false;
		}

		if (article.getTitle() == null || article.getTitle().isEmpty()) {
			return false;
		}

		return true;
	}

	/**
	 * Helper method that given some text removes stopwords and tokenises them, and
	 * stores the statistics.
	 */
	public Void process(String text, NewsStatistic stats) {
		BaseTermPipelineAccessor termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords", "PorterStemmer");
		Tokeniser tokeniser = Tokeniser.getTokeniser();
		String[] inputTokens = tokeniser.getTokens(text);

		if (inputTokens == null)
			return null;

		HashMap<String, Integer> finalMap = stats.getTermFrequencyMap();

		for (int i = 0; i < inputTokens.length; i++) {
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);

			stats.incrementDocLenght();
			
			if (processedTerm == null || !allQueryTerms.contains(processedTerm))
				continue;

			if (finalMap.containsKey(inputTokens[i])) {
				finalMap.put(inputTokens[i], finalMap.get(inputTokens[i]) + 1);
			} else {
				finalMap.put(inputTokens[i], 1);
			}
		}

		stats.setTermFrequencyMap(finalMap);
		return null;
	}
}