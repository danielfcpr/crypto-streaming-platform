COMPOSE = docker compose -p crypto-streaming-platform

up:
	$(COMPOSE) up -d zookeeper kafka kafdrop kafka-connect ingest_coingecko spark spark-worker airflow-postgres airflow-webserver airflow-scheduler

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

spark-silver:
	$(COMPOSE) run --rm spark-silver

spark-gold:
	$(COMPOSE) run --rm spark-gold

athena-apply:
	$(COMPOSE) run --rm athena-apply

athena-repair:
	$(COMPOSE) run --rm athena-repair

clean:
	$(COMPOSE) down -v

airflow-up:
	docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler

airflow-logs:
	docker compose logs -f --tail=100 airflow-webserver airflow-scheduler