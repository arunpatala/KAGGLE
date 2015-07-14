import com.github.tototoshi.csv._
import scala.util.Random
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

object Main{

	val pid = "PassengerId"
	val s = "Survived"
	val lst = read
	val size = lst.size
	val rand = new Random(0)

	def read = {
		CSVReader.open("data/train.csv")
		.iteratorWithHeaders.toList
	}
	def getData = lst.map{m=>toLabeledPoint(m)}.toList

	def toLabeledPoint(m:Map[String,String])= {
		val gender = if(m("Sex")=="male") 1.0 else 0.0
		val cls = m("Pclass").toDouble
		val fare = if(m("Fare")=="") -100.0 else m("Fare").toDouble
		val age = if(m("Age")=="") -100.0 else m("Age").toDouble
		val survived = m(s).toDouble
		LabeledPoint(survived,new DenseVector(Array(gender,cls,fare,age)))
	}

	val prob = {
		val map = lst.groupBy(_(s)).mapValues(_.size)		
		map("1").toDouble/size
	}

	def pred0(m:Map[String,String]):Int = {
		if(rand.nextBoolean) 1 else 0	
	}

	def pred1(m:Map[String,String]):Int = {
		if(rand.nextDouble<prob) 1 else 0	
	}

	def write(name:String) = {
		val test = CSVReader.open("data/test.csv")
			.iteratorWithHeaders.toList
		val out = CSVWriter.open(""+name+".csv")
		val pred = test.map{s=>List(s(pid),predRF(s).toInt)}.toList
		out.writeRow(List(pid,s))
		out .writeAll(pred)
		out.close()
	}

	def predRF(m:Map[String,String]) = {
		val point = toLabeledPoint(m+(s->"1"))
		RF.model.predict(point.features)
	}


}
