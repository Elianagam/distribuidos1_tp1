# distribuidos1_tp1

Run app:

```
make docker-image
make docker-compose-up
make docker-compose-logs
make docker-compose-down
```

Run client_test:

```
docker build -t client_net . 

docker run --rm -it --network=distribuidos1_tp1_metrics_network client_net

```