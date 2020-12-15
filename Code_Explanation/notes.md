### Note 1) Parameterising the implicit Encoder 

- First, we build a generic function to read a csv file and return a dataset with the type [T]. To manipulate Row objects into domain specific ones, we use case class
to type the collections. Look at a code below for clarity of our intention behind the code. 

```scala

trait BaseCode[T] {
  def readFile(spark:SparkSession,separator:String):Dataset[T]={
    import spark.implicits._
    spark.read.option("sep",separator)
      .schema(getSchema(colNames))
      .csv(fileName)
      .as[T]
  }
}
```

The code itself does not look bad on the surface. However, if you look at it more closely, we encounter the error saying "no implicity arguement of type: Encoder [T]".
This type of error is due to the fact that the above code does not gurantee that there will be Encoder[T] is available when necessary. (Encoder is used to convert a JVM object 
from or to a spark representation). To get around a problem is simply   _to delay the moment when the complier will try to find the required Encoder by parameterizing the implicit encoder_.

The modified code is as fllows;

```scala
trait BaseCode[T] {
  def readFile(spark:SparkSession,separator:String)(implicit enc:Encoder[T]):Dataset[T]={
    import spark.implicits._
    spark.read.option("sep",separator)
      .schema(getSchema(colNames))
      .csv(fileName)
      .as[T]
  }
}
```
Of course, you will need to bring the encoder in scope when calling the method. 
For example, in our case, you need to specify collections by using case calls MovieTitles and generate code like Encoder[MovieTitles]. 

- Second , import SparkSession.implicits not only in the place where we define the function but also where you call it. That is, there is another import that bring the encoder
into scope than  import SparkSession.implicits._ which we have in the pace we define it. 


