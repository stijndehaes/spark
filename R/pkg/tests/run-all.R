#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

library(testthat)
library(SparkR)

# SPARK-25572
if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # Turn all warnings into errors
  options("warn" = 2)

  if (.Platform$OS.type == "windows") {
    Sys.setenv(TZ = "GMT")
  }

  # Setup global test environment
  # Install Spark first to set SPARK_HOME

  # NOTE(shivaram): We set overwrite to handle any old tar.gz files or directories left behind on
  # CRAN machines. For Jenkins we should already have SPARK_HOME set.
  install.spark(overwrite = TRUE)

  sparkRDir <- file.path(Sys.getenv("SPARK_HOME"), "R")
  sparkRWhitelistSQLDirs <- c("spark-warehouse", "metastore_db")
  invisible(lapply(sparkRWhitelistSQLDirs,
                   function(x) { unlink(file.path(sparkRDir, x), recursive = TRUE, force = TRUE)}))
  sparkRFilesBefore <- list.files(path = sparkRDir, all.files = TRUE)

  sparkRTestMaster <- "local[1]"
  sparkRTestConfig <- list()
  if (identical(Sys.getenv("NOT_CRAN"), "true")) {
    sparkRTestMaster <- ""
  } else {
    # Disable hsperfdata on CRAN
    old_java_opt <- Sys.getenv("_JAVA_OPTIONS")
    Sys.setenv("_JAVA_OPTIONS" = paste("-XX:-UsePerfData", old_java_opt))
    tmpDir <- tempdir()
    tmpArg <- paste0("-Djava.io.tmpdir=", tmpDir)
    sparkRTestConfig <- list(spark.driver.extraJavaOptions = tmpArg,
                             spark.executor.extraJavaOptions = tmpArg)
  }

  test_package("SparkR")

  if (identical(Sys.getenv("NOT_CRAN"), "true")) {
    # set random seed for predictable results. mostly for base's sample() in tree and classification
    set.seed(42)

    # TODO (SPARK-30663) To be removed once testthat 1.x is removed from all builds
    if (grepl("^1\\..*", packageVersion("testthat"))) {
      # testthat 1.x
      test_runner <- testthat:::run_tests
      reporter <- "summary"

    } else {
      # testthat >= 2.0.0
      test_runner <- testthat:::test_package_dir
      reporter <- testthat::default_reporter()
    }

    test_runner("SparkR",
                file.path(sparkRDir, "pkg", "tests", "fulltests"),
                NULL,
                reporter)
  }

  SparkR:::uninstallDownloadedSpark()

}
