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

EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/gojob"]
