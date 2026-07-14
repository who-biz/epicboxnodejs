## Nginx Passthrough/Proxy Example

To expose your Epicbox Docker service securely on your own domain, use a simple nginx reverse proxy. This allows you to use SSL and a custom domain name.

### 1. Start Epicbox Docker with Custom Domain

```sh
EPICBOX_DOMAIN=your-epicbox-domain.example docker compose up -d --build
```

### 2. Example nginx Reverse Proxy Configuration

Place this in your nginx config (e.g., `/etc/nginx/sites-available/epicbox.conf`):

```
upstream epicbox_backend {
  ip_hash;
  server 127.0.0.1:8888 max_fails=3 fail_timeout=10s;
  server 127.0.0.1:8889 max_fails=3 fail_timeout=10s;
}
server {
	server_name your-epicbox-domain.example www.your-epicbox-domain.example;

	root /var/www/html/epicbox/;
	index index.html index.htm;

	location / {
		proxy_set_header        Host $host;
		proxy_set_header        X-Real-IP $remote_addr;
		proxy_set_header        X-Forwarded-For $proxy_add_x_forwarded_for;
		proxy_set_header        X-Forwarded-Proto $scheme;

		proxy_pass http://epicbox_backend;
		proxy_read_timeout  90;

		# WebSocket support
		proxy_http_version 1.1;
		proxy_set_header Upgrade $http_upgrade;
		proxy_set_header Connection "upgrade";
	}

	access_log /var/log/nginx/epicbox.access.log;
	error_log /var/log/nginx/epicbox.error.log;

	listen 443 ssl;
	ssl_certificate /etc/letsencrypt/live/your-epicbox-domain.example/fullchain.pem;
	ssl_certificate_key /etc/letsencrypt/live/your-epicbox-domain.example/privkey.pem;
	include /etc/letsencrypt/options-ssl-nginx.conf;
	ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}
```

- Replace `your-epicbox-domain.example` with your actual domain.
- Adjust SSL certificate paths as needed.

### 3. Reload nginx

```sh
sudo nginx -s reload
```

Now, your domain will securely proxy to your Epicbox Docker service!
# epicboxnodejs

Epicbox Relay Server for Epic Cash, built with Node.js and Rust.

## Docker Quick Start

1. **Clone the repository, recursively pulling in submodules:**
	```sh
	git clone <repo-url> --recursive
	```

2. **Create your `.env` file (required):**
	Database credentials are not stored in the repository. Compose will not start without them:
	```sh
	cp .env.example .env
	```
	Edit `.env` and set strong values for `MONGO_ROOT_USER`, `MONGO_ROOT_PASSWORD`, and `EPICBOX_DB_PASSWORD` (e.g. `openssl rand -hex 48`)

3. **Build and start all services with Docker Compose:**
	```sh
	docker compose up -d --build
	```

4. **Custom configuration via environment variables:**
	You can override key settings at runtime:
	```sh
	EPICBOX_DOMAIN=my.domain.com docker compose up -d --build
	```
	- `EPICBOX_DOMAIN`: Sets the domain for epicbox services (default: epicbox.your-domain.com)
	- `EPICBOX_PORT`: Sets the port for epicbox services (default: 443)
	- `NGINX_PORT`: Sets the external port for nginx (default: 8443)

5. **Access the service:**
	- Open `https://localhost:8443` (or your chosen NGINX_PORT) in your browser.

6. **Scaling and failover:**
	- Two epicbox instances are started by default (epicbox1 and epicbox2).
	- nginx will automatically route requests to available instances.

## Configuration Reference

All major settings can be configured via environment variables or a `.env` file:

```
MONGO_ROOT_USER=root
MONGO_ROOT_PASSWORD=<strong password>
EPICBOX_DB_PASSWORD=<strong password>
EPICBOX_DOMAIN=my.domain.com
EPICBOX_PORT=443
```

## Advanced

- MongoDB, nginx, and epicbox instances are all managed via `docker-compose.yml`.
- For custom setups, edit `docker-compose.yml` and `default_config.json` as needed.
