FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /bunny-prometheus-exporter .

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /bunny-prometheus-exporter /bunny-prometheus-exporter
EXPOSE 9877
ENTRYPOINT ["/bunny-prometheus-exporter"]
