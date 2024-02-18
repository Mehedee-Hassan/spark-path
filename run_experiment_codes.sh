
#!/bin/bash

#DockercontainernameorIDrunningSpark
CONTAINER_NAME="spark-master-experiment"

#PathtoyourPySparkapplicationinsidethecontainer
APP_PATH="/home/app/"

#Applicationarguments
#Replacethesewithanyargumentsyourapplicationexpects
ARG1="$1"
ARG2="$2"

#RunningtheSparkapplication
docker exec -it "$CONTAINER_NAME" spark-submit "$APP_PATH""$ARG1"