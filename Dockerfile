FROM golang:1.10.5-stretch
WORKDIR /go/src/github.com/davefinster/rcj-go
COPY . .
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh && \
    dep ensure -v && \
    GOOS=linux GOARCH=amd64 go build -a -v main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/davefinster/rcj-go/main .
EXPOSE 8080
CMD ["./main"]
