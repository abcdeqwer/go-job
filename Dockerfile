FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO off so the result runs on a scratch-adjacent base; the MySQL driver is pure Go.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gojob ./cmd/gojob

FROM alpine:3.20
# tzdata, because the business Location is looked up by name and a scheduler that silently
# falls back to UTC is a scheduler that fires at the wrong hour without saying so.
RUN apk add --no-cache tzdata ca-certificates
COPY --from=build /gojob /usr/local/bin/gojob

# Where browser setup writes the control DSN.
#
# On a path that is a declared volume, so it survives `docker restart`. It does NOT survive the
# container being RECREATED — `compose up -d` after a rebuild makes a new anonymous volume — so
# a compose deployment should either mount this path itself or, better, set GOJOB_CONTROL_DSN
# and never need the file at all. The setup page says so at the moment it writes it.
ENV GOJOB_CONFIG=/var/lib/gojob/gojob.json
RUN mkdir -p /var/lib/gojob
VOLUME /var/lib/gojob

EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/gojob"]
