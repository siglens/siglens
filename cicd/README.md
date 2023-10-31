## CI/CD

Contains files used by ci/cd

`ingest.csv`: Queries to send against siglens for queries right after the data was ingested. This means that some buffers may have not been flushed, so this test is slightly more lenient.

`restart.csv`: Queries to send against siglens after restart. This means all data has to exist, so this test is stricter.