### Run


```commandline
mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=otel.OtelDemo \
    -Dexec.args="--project=<project> \
    --gcpTempLocation=gs://<bucket> \
    --runner=DataflowRunner \
    --region=us-central1"

```