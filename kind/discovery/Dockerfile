# The data plane for kind/discovery.sh: the release binary the script builds on the host,
# alone on the host's own distribution, so that the glibc it was linked against is the one
# it runs on. The build context is a directory holding nothing but the binary.
FROM BASE
COPY edgerush /usr/local/bin/edgerush
USER nobody
ENTRYPOINT ["edgerush"]
