# Prepend/Append plugin parcel classpaths

if [ "$HADOOP_USER_CLASSPATH_FIRST" = 'true' ]; then
  # HADOOP_CLASSPATH={{HADOOP_CLASSPATH_APPEND}}
  :
else
  # HADOOP_CLASSPATH={{HADOOP_CLASSPATH}}
  :
fi
# JAVA_LIBRARY_PATH={{JAVA_LIBRARY_PATH}}

export HADOOP_CLASSPATH="HADOOP_CLASSPATH:/opt/hadoop/share/hadoop/tools/lib/*"
export HADOOP_CLIENT_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"
