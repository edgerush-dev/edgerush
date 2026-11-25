# The planes' image for the kind scripts: the release binaries a script builds on the host,
# `edgerush` and the rate-limiting service's beside it (docs/05 §7), alone on the host's
# own distribution, so that the glibc they were linked against is the one they run on. The
# build context is a directory holding nothing but the two binaries.
FROM BASE
COPY edgerush edgerush-ratelimit-service /usr/local/bin/
USER nobody
ENTRYPOINT ["edgerush"]
