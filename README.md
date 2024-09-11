# How to use

Run the following to rollover a data stream

```bash
python ./opensearch-helpers/data_stream.py rollover \
    --url https://<your-opensearch-url> \
    --username admin \
    --password <admin_password> \
    --data-stream <data_stream_name>;
```

Run the following to remove indices older than 7 days

```bash
python ./opensearch-helpers/data_stream.py clean \
    --url https://<your-opensearch-url> \
    --username admin \
    --password <admin_password> \
    --data-stream <data_stream_name> \
    --retention-period 7;
```