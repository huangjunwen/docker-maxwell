# Compile controller
FROM golang:1.13

WORKDIR /go/src/github.com/huangjunwen/docker-maxwell/controller

COPY controller/* .

RUN CGO_ENABLED=0 go build -o controller

# maxwell + redis + controller
FROM ubuntu:disco

ENV MAXWELL_VER 1.23.5

ENV PATH /usr/local/maxwell/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      wget \
      default-jre-headless \
      redis-server && \
      cd /tmp && \
      wget https://github.com/zendesk/maxwell/releases/download/v$MAXWELL_VER/maxwell-$MAXWELL_VER.tar.gz && \
      tar xfz maxwell-$MAXWELL_VER.tar.gz && \
      mv maxwell-$MAXWELL_VER /usr/local/maxwell && \
      rm /tmp/maxwell-$MAXWELL_VER.tar.gz

COPY --from=0 /go/src/github.com/huangjunwen/docker-maxwell/controller/controller /usr/local/bin

ENTRYPOINT ["controller"]
