FROM reg.docker.alibaba-inc.com/kmonitor/golang:1.15
WORKDIR /tsbs
RUN export GOPROXY=https://goproxy.io

COPY ./ ./
RUN go build -o ./bin/benchmark ./cmd/tsbs_load
RUN go build -o ./bin/server ./server

