FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/cloud-tasks-emulator .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/cloud-tasks-emulator /cloud-tasks-emulator
EXPOSE 8123
ENTRYPOINT ["/cloud-tasks-emulator"]
CMD ["-host", "0.0.0.0", "-port", "8123"]
