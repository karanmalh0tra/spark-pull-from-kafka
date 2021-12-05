import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ses.SesClient

import HelperUtils.{CreateLogger, ObtainConfigReference}

import javax.mail.{Address, Message, MessagingException, Session}
import javax.mail.internet.AddressException
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import javax.mail.internet.MimeMultipart
import javax.mail.internet.MimeBodyPart
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Properties
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.ses.model.SendRawEmailRequest
import software.amazon.awssdk.services.ses.model.RawMessage
import software.amazon.awssdk.services.ses.model.SesException

import java.util.UUID;


/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    consumer-group topic1,topic2
 */
object KafkaSparkIntegration {
  val logger = CreateLogger(classOf[KafkaSparkIntegration])
  def main(args: Array[String]): Unit = {
    /* Load Configs from application.conf and Create Logger */
    logger.info("Fetching all the Configurations...")
    val config = ConfigFactory.load()
    val logger = CreateLogger(classOf[KafkaSparkIntegration])

    /* LOAD AWS Configs */
    val FROM: String = config.getString("aws.FROM")
    val TO: String = config.getString("aws.TO")
    val SUBJECT: String = config.getString("aws.SUBJECT")
    val TEXTBODY: String = config.getString("aws.TEXTBODY")

    /* LOAD Spark Configs */
    val brokers = config.getString("spark.BROKERS")
    val topics = config.getString("spark.TOPIC")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaSparkIntegration").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    /*Configure to display ERROR Logs */
    ssc.sparkContext.setLogLevel("ERROR")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> UUID.randomUUID().toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> config.getString("spark.SSL"),
      config.getString("spark.SSL_TRUSTSTORE") -> config.getString("spark.TRUSTSTORE_LOCATION"))
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Fetch Each Line as Received
    val lines = messages.map(_.value)

    // Loop on all the lines received in the interval and send an EMAIL for each one of them
    logger.info("Iterating through the RDD...")
    lines.foreachRDD(x => {
      if (x.count() > 0){
        val HTMLBODY: String = s"""<h1>WARNING/ERROR IN YOUR APPLICATION</h1
                                 <p>You received a WARNING/ERROR LOG in your application</p>
                                 <p><b>${x.collect().mkString(" ")}</b></p>"""
        val client = SesClient.builder()
          .region(Region.US_EAST_1)
          .build()
        try{
          send(client,FROM,TO,SUBJECT,TEXTBODY,HTMLBODY)
          client.close()
        } catch {
          case e: IOException => println(e.getStackTrace())
          case eb: MessagingException => print(eb.getStackTrace)
        }
      }
    })
    // Start the computation
    ssc.start()
    // Await User's termination Command
    ssc.awaitTermination()
  }
  def send(client: SesClient, sender: String, recipient: String, subject: String, bodyText: String, bodyHTML: String): Unit ={


    logger.info("Sending an Email now...")
    // Create a Session and a message of MIME TYPE
    val session: Session = Session.getDefaultInstance(new Properties())
    val message: MimeMessage = new MimeMessage(session)

    // Set Subject, Address, Sender and Recepient
    message.setSubject(subject,"UTF-8")
    val addressArray = InternetAddress.parse(recipient).asInstanceOf[Array[Address]]
    message.setFrom(new InternetAddress(sender))
    message.setRecipients(Message.RecipientType.TO,addressArray)

    // As an alternative to the HTML Body Message in case of failure
    val msgBody: MimeMultipart = new MimeMultipart("alternative")

    val wrap: MimeBodyPart = new MimeBodyPart()

    val textPart = new MimeBodyPart()
    textPart.setContent(bodyText,"text/plain; charset=UTF-8")

    val htmlPart: MimeBodyPart = new MimeBodyPart()
    htmlPart.setContent(bodyHTML,"text/html; charset=UTF-8")

    msgBody.addBodyPart(textPart)
    msgBody.addBodyPart(htmlPart)

    wrap.setContent(msgBody)

    val msg: MimeMultipart = new MimeMultipart("mixed")

    message.setContent(msg)

    msg.addBodyPart(wrap)

    try {
      // Attempt to send an email
      logger.info("Attempting to send an email through Amazon SES using the AWS SDK")
      val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
      message.writeTo(outputStream)
      val buf: ByteBuffer = ByteBuffer.wrap(outputStream.toByteArray)

      val arr: Array[Byte] = new Array[Byte](buf.remaining)
      buf.get(arr)

      val data: SdkBytes = SdkBytes.fromByteArray(arr)
      val rawMessage: RawMessage = RawMessage.builder()
        .data(data)
        .build()
      val rawEmailRequest: SendRawEmailRequest = SendRawEmailRequest.builder()
        .rawMessage(rawMessage)
        .build()
      client.sendRawEmail(rawEmailRequest)
    } catch {
      case e: SesException => {
        System.err.println(e.awsErrorDetails().errorMessage())
        System.exit(1)
      }
    }
  }
}