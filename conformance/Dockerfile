# The conformance runner for kind/conformance.sh: the suite's test binary, built here, so
# that Go is in this image alone and no host needs it (docs/04 §7). The build context is
# this directory.
FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY conformance_test.go ./
RUN CGO_ENABLED=0 go test -c -o /conformance.test .

FROM ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944
COPY --from=build /conformance.test /usr/local/bin/conformance.test
USER nobody
