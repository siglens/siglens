FROM golang:1.18-alpine3.17 AS build
WORKDIR /usr/app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=1
ARG TARGETOS TARGETARCH

RUN echo 'https://dl-cdn.alpinelinux.org/alpine/v3.13/main' >> /etc/apk/repositories
RUN apk add gcc musl-dev libc-dev make && \
     cd /usr/app/cmd/siglens && \
     GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-X 'github.com/siglens/siglens/pkg/config/config.Version=${VERSION}'" -o build/siglens

FROM golang:1.18-alpine3.17
RUN apk add shadow
RUN apk add curl

ARG UNAME=siglens
ARG UID=1000
ARG GID=1000
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o $UNAME

WORKDIR /$UNAME
COPY static static
COPY server.yaml .

RUN chown -R $UNAME:$GID static
RUN chown -R $UNAME:$GID /siglens
USER $UNAME

WORKDIR /$UNAME
COPY --from=build /usr/app/cmd/siglens/build/siglens .

USER root
RUN chown $UNAME:$GID siglens

USER $UNAME
CMD ["./siglens", "--config", "server.yaml"]
