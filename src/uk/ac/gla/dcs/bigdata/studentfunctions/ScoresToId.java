package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class ScoresToId implements MapFunction<Tuple3<Query, String, Double>, Query>{
   
	@Override
	public Query call(Tuple3<Query, String, Double> value) throws Exception {
		return value._1();
	}
}
