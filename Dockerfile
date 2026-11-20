# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tickstream .

# Root variant: Fly mounts volumes owned by root, and distroless has no shell to chown them.
FROM gcr.io/distroless/static-debian12
COPY --from=build /tickstream /tickstream
EXPOSE 8080
ENTRYPOINT ["/tickstream"]
