package com.uncovered.spark.model

case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long) extends TimeValued(timestamp: Long) {

  override def toString = this.userId + super.SEP + this.movieId +
    SEP + this.tag + super.toString
}

case class TagMovies(tag: String, movieIds:List[Int]) {

  val SEP = "++"

  override def toString = tag +
    SEP + movieIds.mkString("|")
}

object TagCreator {

  def lineToTag(line: String): Tag = {
    val fields = line.split(",")
    val tmpMsg = new java.lang.StringBuffer

    val tag =
      if (fields.length == 4) fields(2)
      else if (fields.length > 4) {
        for (i <- 2 to fields.length - 2)
          tmpMsg.append(" ").append(fields(i))
          tmpMsg.toString.replace("\"","").trim()
      }
      else tmpMsg.toString

    new Tag(fields(0).toInt, fields(1).toInt, tag, fields(fields.length - 1).toLong)
  }
}
