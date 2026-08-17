.PHONY: help venv install broker-up broker-down test producer pipeline sink dashboard teardown

help:
	@echo "WikiPulse — real-time Wikipedia edit-spike detector"
	@echo ""
	@echo "  make install      create venv + install deps"
	@echo "  make broker-up    start local Redpanda (docker)"
	@echo "  make test         run unit tests"
	@echo "  make pipeline     run the stream processor (terminal 1)"
	@echo "  make sink         run the DuckDB sink        (terminal 2)"
	@echo "  make producer     run the Wikipedia producer (terminal 3)"
	@echo "  make dashboard    launch the Streamlit dashboard"
	@echo "  make broker-down  stop + wipe local Redpanda"
	@echo "  make teardown     stop everything (see scripts/teardown.sh)"

install:
	python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

broker-up:
	docker compose up -d

broker-down:
	docker compose down -v

test:
	. .venv/bin/activate && python -m pytest -q

producer:
	. .venv/bin/activate && python -m src.producer

pipeline:
	. .venv/bin/activate && python -m src.pipeline

sink:
	. .venv/bin/activate && python -m src.sink

dashboard:
	. .venv/bin/activate && streamlit run src/app.py

teardown:
	bash scripts/teardown.sh
