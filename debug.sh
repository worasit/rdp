mvn compile exec:java \
    -Dexec.mainClass=com.nongped.rdp.UserScore \
    -Dexec.args="--project=nongped-playground \
    --gcpTempLocation=gs://nongped-playground/tmp/ \
    --output=gs://nongped-playground/output \
    --runner=DirectRunner \
    --jobName=dataflow-rdp" \
    -Pdirect-runner