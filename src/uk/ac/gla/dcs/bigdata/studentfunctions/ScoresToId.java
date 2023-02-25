package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class ScoresToId implements MapFunction<Tuple2<Query, RankedResult>, Query>{
   
	@Override
	public Query call(Tuple2<Query, RankedResult> value) throws Exception {
		return value._1();
	}
}
