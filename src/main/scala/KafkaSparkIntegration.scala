import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ses.SesClient
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
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: KafkaSparkIntegration <brokers> <groupId> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <groupId> is a consumer group name to consume from topics
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }
    /* AWS START */
    val FROM: String = "kmalho4@uic.edu"
    val TO: String = "m9.karan@gmail.com"
    val SUBJECT: String = "Amazon SES Warning/Error Log in Your Application"
    val TEXTBODY: String = "This email was sent through Amazon SES using the AWS SDK for Scala"
    /* AWS Config End */

//    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaSparkIntegration").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    lines.print()
    lines.foreachRDD(x => {
      if (x.count() > 0){
        val HTMLBODY: String = s"""<h1>WARNING/ERROR IN YOUR APPLICATION</h1
                                 |<p>This email was sent with <a href="https://aws.amazon.com/sdk-for-java/">AWS SDK for Java</a>
                                 |<p>You received a WARNING/ERROR Message</p>
                                 |<p>${x.collect().mkString(" ")}</p>"""
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
    ssc.awaitTermination()
  }
  def send(client: SesClient, sender: String, recipient: String, subject: String, bodyText: String, bodyHTML: String): Unit ={
    val session: Session = Session.getDefaultInstance(new Properties())
    val message: MimeMessage = new MimeMessage(session)

    message.setSubject(subject,"UTF-8")
    val toAddress:Address = new InternetAddress(recipient).asInstanceOf[Address]
    message.setFrom(new InternetAddress(sender))
    message.setRecipient(Message.RecipientType.TO,toAddress)

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
      println("Attempting to send an email through Amazon SES using the AWS SDK")
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
