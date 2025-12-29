COMPOSE = docker compose

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

spark-silver:
	$(COMPOSE) run --rm spark-silver

spark-gold:
	$(COMPOSE) run --rm spark-gold

clean:
	$(COMPOSE) down -v
