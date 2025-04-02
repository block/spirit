FROM golang:1.24 as builder

RUN go install github.com/mfridman/tparse@latest

# copy the installed tparse binary to the final image
FROM golang:1.24

COPY --from=builder /go/bin/tparse /usr/local/bin/tparse

WORKDIR /app

COPY ../go.mod go.sum ./

RUN go mod download

COPY .. .
