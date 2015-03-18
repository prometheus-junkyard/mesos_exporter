FROM golang:onbuild
MAINTAINER Prometheus Team <prometheus-developers@googlegroups.com>

ADD config.json /config.json

ENTRYPOINT ["go-wrapper", "run" ]
CMD        [ "-config.file=/config.json" ]
EXPOSE     9105
