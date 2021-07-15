FROM gcr.io/distroless/static:nonroot

COPY statefulset-resize-controller /usr/local/bin/src

ENTRYPOINT [ "/usr/local/bin/src" ]
