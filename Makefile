COMPOSE = docker compose

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

spark-silver:
	$(COMPOSE) run --rm spark-submit

connect-s3:
	@curl -s $(CONNECT_URL)/connectors/$(CONNECTOR_NAME) >/dev/null 2>&1 && \
	  curl -s -X PUT -H "Content-Type: application/json" --data @$(CONNECTOR_FILE) \
	    $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/config || \
	  curl -s -X POST -H "Content-Type: application/json" --data @$(CONNECTOR_FILE) \
	    $(CONNECT_URL)/connectors
	@echo "Connector applied: $(CONNECTOR_NAME)"
	@curl -s $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/status | python -m json.tool || true

clean:
	$(COMPOSE) down -v
