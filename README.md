# ccxt-proxy
  * mkdir ~/data
  * cd ~/data
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
  * docker build -t ccxt_proxy .
  * docker run -d -p 8000:8000 -v ~/ccxt_proxy:/app/data ccxt_proxy
# doc
  * [doc.md](doc.md)
