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
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.informers.ResourceEventHandler

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.Logging

private[k8s] trait LoggingPodStatusWatcher extends ResourceEventHandler[Pod] {
  def watchOrStop(podName: String, podNamespace: String): Unit
}

/**
 * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
 * every state change and also at an interval for liveness.
 *
 * @param conf kubernetes driver conf.
 */
private[k8s] class LoggingPodStatusWatcherImpl(conf: KubernetesDriverConf)
  extends LoggingPodStatusWatcher with Logging {

  private val appId = conf.appId

  private var podCompleted = false

  private var pod = Option.empty[Pod]
  private var podName = Option.empty[String]
  private var podNamespace = Option.empty[String]

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("unknown")

  def isDriverPod(pod: Pod): Boolean = {
    (podName.isDefined && podNamespace.isDefined && pod.getMetadata.getName == podName.get
      && pod.getMetadata.getNamespace == podNamespace.get)
  }

  def updateStatus(pod: Pod): Unit = {
    if (isDriverPod(pod)) {
      this.pod = Some(pod)
      logLongStatus()
      if (hasCompleted()) {
        closeWatch()
      }
    }
  }

  def onAdd(pod: Pod) {
    updateStatus(pod)
  }

  def onUpdate(oldObj: Pod, pod: Pod) {
    updateStatus(pod)
  }

  def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
    if (isDriverPod(pod)) {
      this.pod = Some(pod)
      closeWatch()
    }
  }

  private def logLongStatus(): Unit = {
    logInfo(
      "State changed, new state: " + pod
        .map(formatPodState)
        .getOrElse("unknown")
    )
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = synchronized {
    podCompleted = true
    this.notifyAll()
  }

  override def watchOrStop(podName: String, podNamespace: String): Unit = {
    val sId = Seq(conf.namespace, podName).mkString(":")
    this.podName = Some(podName)
    this.podNamespace = Some(podNamespace)
    if (conf.get(WAIT_FOR_APP_COMPLETION)) {
      logInfo(
        s"Waiting for application ${conf.appName} with submission ID $sId to finish..."
      )
      val interval = conf.get(REPORT_INTERVAL)
      synchronized {
        while (!podCompleted) {
          wait(interval)
          logInfo(s"Application status for $appId (phase: $phase)")
        }
      }
      logInfo(
        pod
          .map { p =>
            s"Container final statuses:\n\n${containersDescription(p)}"
          }
          .getOrElse("No containers were found in the driver pod.")
      )
      logInfo(s"Application ${conf.appName} with submission ID $sId finished")
    } else {
      logInfo(
        s"Deployed Spark application ${conf.appName} with submission ID $sId into Kubernetes"
      )
    }
  }
}
