# aws modules

## aws-kinesis-service

The model is used to test Kinesis locally.

### To Run Localstack - AWS Kinesis

docker run \
  --name localstack \
  -e "SERVICES=kinesis" \
  -e "DEFAULT_REGION=us-east-1" \
  -e "HOSTNAME=localhost" \
  -e "USE_SSL=false" \
  -e "KINESIS_ERROR_PROBABILITY=0.0" \
  -p 4566:4566 -p 4571:4571 localstack/localstack