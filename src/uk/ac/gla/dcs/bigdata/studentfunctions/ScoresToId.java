package src.uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;


/**
 * Class that returns the id of an article.
 * 
 * @author Davide, Manuel, Paul
 */
public class ScoresToId implements MapFunction<Tuple2<Query, RankedResult>, Query>{
   
	private static final long serialVersionUID = 4648855946428966378L;

	@Override
	public Query call(Tuple2<Query, RankedResult> value) throws Exception {
		return value._1();
	}
}
