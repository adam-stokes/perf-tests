export PATH="$WORKSPACE/bin:$PATH"
DOCKER_RUN="docker run -v `pwd`:`pwd` -v ${SSH_AUTH_SOCK}:${SSH_AUTH_SOCK} -e SSH_AUTH_SOCK=${SSH_AUTH_SOCK} -w `pwd` -t oblt-perf:latest"

