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
package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.{Semaphore, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class GlutenSparkSessionJobTaggingAndCancellationSuite
  extends SparkSessionJobTaggingAndCancellationSuite
  with GlutenTestSetWithSystemPropertyTrait
  with Logging {

  testGluten("GLUTEN-DEBUG Tags set from session are prefixed with session UUID") {
    sc = new SparkContext("local[2]", "test")
    val session = classic.SparkSession.builder().sparkContext(sc).getOrCreate()
    import session.implicits._

    val mainThread = Thread.currentThread()
    logError(
      s"[GLUTEN-DEBUG] Main thread: ${mainThread.getName} " +
        s"(id=${mainThread.getId}, class=${mainThread.getClass.getName})")

    val sem = new Semaphore(0)
    val jobTags = new java.util.concurrent.ConcurrentHashMap[Int, String]()
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val tags = Option(jobStart.properties.getProperty(SparkContext.SPARK_JOB_TAGS))
          .getOrElse("<null>")
        jobTags.put(jobStart.jobId, tags)
        logError(s"[GLUTEN-DEBUG] onJobStart: jobId=${jobStart.jobId}, tags=$tags")
        sem.release()
      }
    })

    session.addTag("one")
    val mainManagedTags = session.managedJobTags.get().toMap
    val mainThreadUuid = session.threadUuid.get()
    logError(s"[GLUTEN-DEBUG] Main thread managedJobTags: $mainManagedTags")
    logError(s"[GLUTEN-DEBUG] Main thread threadUuid: $mainThreadUuid")

    Future {
      val futureThread = Thread.currentThread()
      val futureManagedTags = session.managedJobTags.get().toMap
      val futureThreadUuid = session.threadUuid.get()
      logError(
        s"[GLUTEN-DEBUG] Future thread: ${futureThread.getName} " +
          s"(id=${futureThread.getId}, class=${futureThread.getClass.getName})")
      logError(s"[GLUTEN-DEBUG] Future thread managedJobTags: $futureManagedTags")
      logError(s"[GLUTEN-DEBUG] Future thread threadUuid: $futureThreadUuid")
      logError(
        s"[GLUTEN-DEBUG] Future thread inherited tag 'one': " +
          s"${futureManagedTags.contains("one")}")

      session.range(1, 10000).map { i => Thread.sleep(100); i }.count()
    }(ExecutionContext.global)

    val acquired = sem.tryAcquire(1, 1, TimeUnit.MINUTES)
    logError(s"[GLUTEN-DEBUG] Semaphore acquired: $acquired")
    logError(s"[GLUTEN-DEBUG] All job tags observed: $jobTags")

    val realTag = session.managedJobTags.get()("one")
    logError(s"[GLUTEN-DEBUG] Looking for tag: $realTag")

    val activeJobsFuture =
      session.sparkContext.cancelJobsWithTagWithFuture(realTag, "reason")
    val cancelledJobs = ThreadUtils.awaitResult(activeJobsFuture, 60.seconds)
    logError(s"[GLUTEN-DEBUG] Cancelled jobs count: ${cancelledJobs.size}")
    cancelledJobs.foreach(job => logError(s"[GLUTEN-DEBUG] Cancelled job: ${job.jobId}"))

    assert(
      cancelledJobs.nonEmpty,
      s"Expected at least one cancelled job. " +
        s"mainTags=$mainManagedTags, realTag=$realTag, observedJobTags=$jobTags")

    val activeJob = cancelledJobs.head
    val actualTags = activeJob.properties
      .getProperty(SparkContext.SPARK_JOB_TAGS)
      .split(SparkContext.SPARK_JOB_TAGS_SEP)
    assert(
      actualTags.toSet == Set(
        session.sessionJobTag,
        s"${session.sessionJobTag}-thread-${session.threadUuid.get()}-one",
        SQLExecution.executionIdJobTag(
          session,
          activeJob.properties
            .get(SQLExecution.EXECUTION_ROOT_ID_KEY)
            .asInstanceOf[String]
            .toLong)
      ))
  }
}
