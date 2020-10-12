FROM golang:1.15
WORKDIR /go/src/kcat

RUN apt-get update && apt-get upgrade -y && apt-get install -y librdkafka-dev librdkafka1 && apt-get clean && \
    go get github.com/confluentinc/confluent-kafka-go/kafka && go get github.com/spf13/pflag

COPY *.go ./
RUN go install -v ./... && sha256sum /go/bin/kcat

CMD /go/bin/kcat
