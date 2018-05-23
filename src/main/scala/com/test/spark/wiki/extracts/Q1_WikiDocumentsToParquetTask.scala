package com.test.spark.wiki.extracts

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.io.Source

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  import session.implicits._

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage
            val doc = Jsoup.connect(url).get
            doc.select("table:has(caption:contains(Classement)) tr:not(:first-child)")
               .map {
                  element =>
                   {
                     val team = element.selectFirst("td a").text()
                     val row = element.select("td").eachText()
                     LeagueStanding(league,season,row(0).toInt,team,row(2).toInt,row(3).toInt,row(4).toInt,row(5).toInt,row(6).toInt,row(7).toInt,row(8).toInt,row(9).toInt)
                   }
               }
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              /**
                * - La console EMR
                * - Spark history server
                * - Yarn UI si spark utilise YARN.
                *
                */


              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    /**
      * Les avantages sont :
      *     -  Portabilité
      *     -  Performances (lecture et écriture)
      *     -  Occupe moins d'espace de stockage
      */

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    /**
      * L'API du Dataset est plus riche que celle d'une séquence scala,
      * la donnée et le traitement associé seront distribués et parallelisés
      * on pourra requetté la donnée plus facilement en SQL, la sauvegarder
      * sur différents supports (fichier, base de données, ...) et sous différents formats.
      *
      */

  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val bufferedSource = Source.fromFile(getClass.getResource("/leagues.yaml").getPath)
    try {
      mapper.readValue(bufferedSource.bufferedReader(), classOf[Array[LeagueInput]]).toSeq
    } finally {
      bufferedSource.close()
    }
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
