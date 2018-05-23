package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")

    standings.createOrReplaceTempView("standingsTable")
    session.sql(
      """ select season, league, round(avg(goalsFor),2) as avg_goalsFor
          from standingsTable group by league,season
          order by season """)
      .show()


    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")

    val team = standings.filter(x=> (x.position==1 && x.league.toLowerCase=="ligue 1"))
                        .groupByKey(_.team).count()
                        .orderBy(desc("count(1)")).first()._1
    println(team)
    println()

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")

    standings.filter(_.position==1)
      .agg(avg($"points")
        .alias("moyenne_points").as[Float])
      .show()


    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?

    val decade: Int => String = _.toString.replaceAll(".$", "X")
    val decadeUDF = udf(decade)

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

    standings.filter(standing => (standing.position==1 || standing.position==10))
      .map(st => ((st.league, st.season),st.points))
      .groupByKey(_._1)
      .mapGroups((k,v) => ((k._1,decade(k._2)),Math.abs(v.map(_._2).reduceLeft(_-_))))
      .groupByKey(_._1)
      .mapGroups((k,v) => (k,{val l = v.map(_._2).toSeq; l.sum/l.length}))
      .withColumnRenamed("_1","championnat_decennie")
      .withColumnRenamed("_2","moyenne_points_ecart")
      .show()

  }
}
