FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /halyard ./cmd/halyard && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /halyard /halyard
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 6379 7000 8080
ENTRYPOINT ["/halyard", "-addr", "0.0.0.0:6379", "-dir", "/data"]
