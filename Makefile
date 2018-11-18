buildProto:
	protoc -I proto proto/robocup.proto --go_out=plugins=grpc,import_path=proto:./api/proto

createDbKube:
	kubectl exec cockroachdb-0 /cockroach/cockroach -- user set rcjgo --insecure && \
  kubectl exec cockroachdb-0 /cockroach/cockroach -- sql --insecure -e 'CREATE DATABASE rcj' && \
  kubectl exec cockroachdb-0 /cockroach/cockroach -- sql --insecure -e 'GRANT ALL ON DATABASE rcj TO rcjgo'


