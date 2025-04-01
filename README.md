# ccxt-proxy
  * mkdir ~/ccxt_proxy
  * cd ~/ccxt_proxy
  * nano config.json

# uv run
  * create file ./data/config.json
  * uv sync
  * uv run python src/main.py
  * uv run python src/main.py test
# Docker Compose
  * create file ~/ccxt_proxy/config.json
  * docker-compose up --build
# Docker Run
  * create file ~/ccxt_proxy/config.json
  * docker run -d -p 4915:8000 -v ~/ccxt_proxy:/app/data -e PYTHONUNBUFFERED=1 --name ccxt-proxy --restart=always hxse/ccxt-proxy:latest`
# doc
  * [doc.md](doc.md)
