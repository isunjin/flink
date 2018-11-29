#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

USAGE="Usage: kubernetes-entry.sh (cluster|taskmanager|job) [args]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

MODE=$1
ARGS=("${@:2}" "--configDir" "${FLINK_CONF_DIR}")

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

CC_CLASSPATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`
CC_CLASSPATH="$FLINK_HOME/libs
log="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-kubernetes-appmaster-${HOSTNAME}.log"
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-console.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback-console.xml"

ENTRY_POINT=org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypointRunner

if [ "$MODE" = "taskmanager" ]; then
    ENTRY_POINT=org.apache.flink.kubernetes.taskmanager.KubernetesTaskManagerRunner
elif [ "$MODE" = "job" ]; then
    ENTRY_POINT=org.apache.flink.kubernetes.entrypoint.KubernetesJobClusterEntrypoint
fi

# Evaluate user options for local variable expansion
FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})

exec $JAVA_RUN $JVM_ARGS $FLINK_ENV_JAVA_OPTS -classpath "$CC_CLASSPATH" $log_setting ${ENTRY_POINT} "${ARGS[@]}"

rc=$?

if [[ $rc -ne 0 ]]; then
    echo "Error while starting the Kubernetes session cluster entry point. Please check ${log} for more details."
fi

exit $rc
