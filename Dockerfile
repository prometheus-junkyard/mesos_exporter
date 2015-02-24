FROM golang:1.4.1-onbuild

ADD config.json /config.json

CMD ["go-wrapper", "run", "-config.file=/config.json" ]
