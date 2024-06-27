# build stage
FROM golang:1.22.3-alpine AS build-env
RUN apk add make git bash build-base
ENV GOPATH=/go
ENV PATH="/go/bin:${PATH}"

WORKDIR /app
COPY go.* .
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build make build

# final stage
FROM alpine
WORKDIR /app 
COPY --from=build-env  /app/build /app

ENTRYPOINT [ "/app/scavjob_manager" ]
