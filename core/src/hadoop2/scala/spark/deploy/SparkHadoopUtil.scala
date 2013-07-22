/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import java.io.IOException
import java.security.PrivilegedExceptionAction

/**
 * Contains util methods to interact with Hadoop from spark.
 */
object SparkHadoopUtil {
  val HDFS_TOKEN_KEY = "SPARK_HDFS_TOKEN"
  val conf = newConfiguration()
  UserGroupInformation.setConfiguration(conf)

  def getUserNameFromEnvironment(): String = {
    // defaulting to -D ...
    System.getProperty("user.name")
  }

  def runAsUser(func: (Product) => Unit, args: Product) {
    runAsUser(func, args, getUserNameFromEnvironment())
  }

  def runAsUser(func: (Product) => Unit, args: Product, user: String) {
    val ugi = UserGroupInformation.createRemoteUser(user)
    if (UserGroupInformation.isSecurityEnabled) {
      Option(System.getenv(HDFS_TOKEN_KEY)) match {
        case Some(s) =>
          ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN)
          val token = new Token[TokenIdentifier]()
          token.decodeFromUrlString(s)
          ugi.addToken(token)
        case None => throw new IOException("Failed to get token in security environment")

      }
    }

    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        func(args)
      }
    })
  }

  def createSerializedToken(): Option[String] = {
    if (UserGroupInformation.isSecurityEnabled) {
      val fs = FileSystem.get(conf)
      val user = UserGroupInformation.getCurrentUser.getShortUserName
      Option(fs.getDelegationToken(user).asInstanceOf[Token[_ <: TokenIdentifier]])
        .map(_.encodeToUrlString())
    } else {
      None
    }
  }

  // Return an appropriate (subclass) of Configuration. Creating config can initializes some hadoop subsystems
  def newConfiguration(): Configuration = new Configuration()

  // add any user credentials to the job conf which are necessary for running on a secure Hadoop cluster
  def addCredentials(conf: JobConf) {}

  def isYarnMode(): Boolean = { false }
}
