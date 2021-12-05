package ConfigAndFeatureTests

import HelperUtils.ObtainConfigReference
import com.typesafe.config.ConfigFactory
import org.hamcrest.CoreMatchers.{hasItems, is}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.junit.{Assert, Test}
import org.junit.Assert.{assertEquals, assertNotNull, assertThat, assertTrue}
import org.scalatest.matchers.must.Matchers.{an, be}
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}

import java.io.File
import javax.mail.Address
import javax.mail.internet.InternetAddress
import scala.io.Source


class TestCases {

  val config = ConfigFactory.load("application.conf")

  //check if configurations load
  @Test
  def testCheckConfig = {
    assertNotNull(config)
  }

  // check if broker TLS exists
  @Test
  def testBrokerTLS = {
    assertTrue(config.getString("spark.BROKERS").length > 0)
  }

  //Validate if From Email Address is of type Address
  @Test
  def testFROMEmailIDInstance = {
    val FROM = new InternetAddress(config.getString("aws.FROM"))
    assertTrue(FROM.isInstanceOf[InternetAddress])
  }

  // Validate if TO Email Addresses are of type Array[Address]
  @Test
  def testRecipientEmailAddresses = {
    val addressArray = InternetAddress.parse(config.getString("aws.TO")).asInstanceOf[Array[Address]]
    assertTrue(addressArray.isInstanceOf[Array[InternetAddress]])
  }

  // Test if kafka.client.truststore.jks exist in the server
  @Test
  def testJKSFileExists = {
    assertTrue(new File(config.getString("spark.TRUSTSTORE_LOCATION")).exists())
  }
}