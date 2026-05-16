# Realtime Chat built with Kafka

A scalable, event-driven chat platform powered by **Apache Kafka**, **FastAPI**, **WebSockets**, and multiple databases (PostgreSQL, MongoDB, Redis). Users can register, log in (email/password or Google), join channels, chat in real time, and load full message history when opening a channel.

![Realtime Chat Architecture](resources/architecture_design.png)

<details>
<summary>Table of Contents</summary>

- [Key Features](#key-features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Service Ports](#service-ports)
- [Installation](#installation)
- [Configuration](#configuration)
- [Google OAuth Setup](#google-oauth-setup)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Security Notes](#security-notes)
- [Development Progress](#development-progress)
- [Future Enhancements](#future-enhancements)
- [Project Background](#project-background)

</details>

## Key Features

### Authentication & accounts
- **User registration** with email, name, and password (bcrypt hashing)
- **Login** with email or username + password
- **Google OAuth** sign-in (optional, configured via environment variables)
- **JWT sessions** signed with RSA keys (RS256)
- **Protected routes** — channels and chat require login
- **Logout** with session cleanup

### Real-time chat
- **WebSocket** messaging with JWT authentication
- **Kafka** pub/sub for scalable message distribution
- **Two WebSocket instances** behind **nginx** load balancing
- **Redis** tracks which user is connected to which WebSocket server
- **Live broadcast** to all members of a channel

### Channels & history
- **Create**, **join**, and **search** channels by name
- **Per-channel chat** with date dividers and message timestamps
- **MongoDB message history** — past messages load when you open a channel
- History persists across refresh, logout/login, and new members joining a channel

## Architecture

| Service | Role |
|---------|------|
| **web_client** | Browser UI (FastAPI + Jinja templates) |
| **login_server** | Registration, login, JWT issuing, Google user provisioning |
| **websocket_server** (×2) | WebSocket connections, Kafka producer, channel requests |
| **nginx_load_balancer** | Load balances WebSocket traffic |
| **message_consumer** | Kafka consumer → fan-out to WebSockets + MongoDB |
| **channel_manager** | Channel CRUD, join, search, message history API |
| **PostgreSQL** | Users, channels, memberships |
| **MongoDB** | Chat message history |
| **Redis** | Active connection routing |
| **Kafka + Zookeeper** | Async message bus (`messages` topic) |

### Message flow

1. Client sends a message over **WebSocket** (`ws://localhost:5001/ws`).
2. **WebSocket server** publishes the message to Kafka topic **`messages`** (channel id as key).
3. **Message consumer** reads from Kafka, loads channel members from PostgreSQL, looks up active WebSocket servers in Redis, and POSTs the message to each recipient's server.
4. **WebSocket server** delivers the message to connected clients.
5. **Message consumer** stores the message in **MongoDB** for history.
6. On channel open, the client requests history via WebSocket → **channel_manager** → MongoDB.

### Communication patterns

- **REST** — auth, registration, channel management, health checks
- **WebSockets** — live chat, channel operations, history fetch
- **Kafka** — decoupled message processing and horizontal scaling

## Technology Stack

| Layer | Technologies |
|-------|----------------|
| Backend | FastAPI, Python 3.11, SQLModel, Authlib |
| Message broker | Apache Kafka, Confluent Platform 7.x |
| Databases | PostgreSQL 17, MongoDB, Redis |
| Frontend | HTML, CSS, JavaScript, Bootstrap 5 |
| Auth | JWT (RS256), bcrypt, Google OpenID Connect |
| Infrastructure | Docker, Docker Compose, nginx |

## Service Ports

| Port | Service |
|------|---------|
| **5004** | Web client (main UI) — **open this in the browser** |
| **5001** | nginx → WebSocket (`ws://localhost:5001/ws`) |
| **5002** | login_server API |
| **5005** | channel_manager API |
| **5432** | PostgreSQL |
| **27017** | MongoDB |
| **6379** | Redis |
| **19092** | Kafka (host access) |
| **22181** | Zookeeper (host access) |

## Installation

### Prerequisites

- [Docker Desktop](https://www.docker.com/get-started) installed and running
- Git

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/kafka-realtime-chat.git
cd kafka-realtime-chat
```

### 2. Environment variables

```bash
cd fastapi_kafka
copy .env.example .env    # Windows
# cp .env.example .env  # macOS / Linux
```

Edit `.env` and set your values (see [Configuration](#configuration)). Google OAuth is optional.

### 3. Generate JWT keys

`private_key.pem` is not committed to git. Generate keys locally:

```bash
cd auxiliar
python generate_rsa_keys.py
cd ..
```

This creates `auxiliar/keys/private_key.pem` and `public_key.pem`.

### 4. Start Kafka and Zookeeper

Run from the **`fastapi_kafka`** directory (not `auxiliar`):

```bash
docker compose -f compose.kafka.yaml up -d
```

Wait ~30 seconds for Kafka to be ready.

### 5. Create the Kafka topic

```bash
docker exec kafka-cluster-kafka-1-1 /bin/kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic messages --partitions 20 --replication-factor 1
```

Verify:

```bash
docker exec kafka-cluster-kafka-1-1 /bin/kafka-topics --bootstrap-server kafka-1:9092 --list
```

You should see `messages` in the list.

### 6. Start all application services

```bash
docker compose up -d --build
```

### 7. Open the app

Use **`http://localhost:5004`** (not `127.0.0.1`) for consistent cookies and Google OAuth.

**Demo accounts** (from seed data, password `secret` for all):

- `olivia.rodrigo`
- `taylor.swift`
- `gracie.abrams`

## Configuration

All secrets live in **`fastapi_kafka/.env`** (never commit this file). See `.env.example` for the template.

| Variable | Description |
|----------|-------------|
| `SESSION_SECRET_KEY` | Signs OAuth session cookies in web_client |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID (optional) |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret (optional) |
| `GOOGLE_REDIRECT_URI` | Must match Google Console exactly, e.g. `http://localhost:5004/auth/google/callback` |

Docker Compose also uses default dev credentials for Postgres/Mongo (`root` / `password`). Change these for production deployments.

## Google OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → **APIs & Services** → **Credentials**.
2. Create an **OAuth 2.0 Client ID** (Web application).
3. **Authorized JavaScript origins:** `http://localhost:5004`
4. **Authorized redirect URIs:** `http://localhost:5004/auth/google/callback`
5. Copy Client ID and Client Secret into `fastapi_kafka/.env`.
6. Rebuild web_client: `docker compose up -d --build web_client`

**Tips for reliable Google login:**
- Always use `http://localhost:5004`
- Complete sign-in in one browser tab (avoid the back button during Google redirect)
- If login fails once, try again in a fresh tab

## Usage

1. **Register** or **log in** (or use Google).
2. On **Channels**, create a channel or search by exact name (e.g. `Channel2`, not channel id).
3. **Join** a channel, then open it to chat.
4. Messages appear in real time for all members; history loads at the top when you enter a channel.

## Troubleshooting

### Messages not broadcasting to other users

**Kafka is probably stopped.** Check:

```bash
docker ps --filter "name=kafka"
```

Both `kafka-cluster-kafka-1-1` and `kafka-cluster-zookeeper-1-1` must be **Up**. If not:

```bash
cd fastapi_kafka
docker compose -f compose.kafka.yaml up -d
docker compose restart message_consumer websocket_server_1 websocket_server_2
```

Refresh chat tabs after Kafka is back.

### `compose.kafka.yaml` not found

Run Docker commands from **`fastapi_kafka`**, not from `auxiliar`.

### Google authentication failed

- Use `http://localhost:5004` only
- Confirm redirect URI in Google Console matches `GOOGLE_REDIRECT_URI` in `.env`
- Rebuild: `docker compose up -d --build web_client`

### Signup / database errors

Ensure PostgreSQL is running: `docker compose ps relational_database`

If you see Postgres 18 volume errors, the project pins **`postgres:17`** in `compose.yaml`.

### Channel not found when searching

Search by **channel name** (e.g. `The Tortured Poets Department`), not numeric id.

## Project Structure

```
kafka-realtime-chat/
├── README.md
├── LICENCE.txt
├── resources/
│   └── architecture_design.png
└── fastapi_kafka/
    ├── compose.yaml              # Main application stack
    ├── compose.kafka.yaml        # Kafka + Zookeeper
    ├── .env.example              # Environment template (commit this)
    ├── .env                      # Your secrets (gitignored)
    ├── login_server/             # Auth API + JWT
    ├── web_client/               # Web UI + Google OAuth
    ├── websocket_server/         # WebSocket + Kafka producer
    ├── message_consumer/         # Kafka consumer + fan-out
    ├── channel_manager/          # Channels + MongoDB history
    ├── nginx/                    # WebSocket load balancer
    ├── databases/
    │   ├── relational_database/init/init_database.sql
    │   └── mongodb/init/mongo_init.js
    └── auxiliar/
        ├── generate_rsa_keys.py
        └── keys/                 # private_key.pem (gitignored)
```

## Security Notes

- **Never commit** `fastapi_kafka/.env` or `auxiliar/keys/private_key.pem`
- Rotate Google OAuth secrets if they were ever exposed in git history
- Default Postgres/Mongo passwords are for **local development only**
- JWT private key must be kept secret in production (Render secrets, etc.)

## Development Progress

- [x] User registration and password login
- [x] JWT authentication (RS256)
- [x] Google OAuth sign-in
- [x] Protected web routes and logout
- [x] WebSocket servers with nginx load balancing
- [x] Kafka message distribution
- [x] Message consumer fan-out via Redis
- [x] Channel create, join, and search
- [x] MongoDB message history (load on channel open)
- [x] Environment-based secrets (`.env`)
- [ ] Production deployment (Render / cloud Kafka)
- [ ] Additional features (read receipts, DMs, etc.)

## Future Enhancements

- Direct messaging between users
- Message read receipts and typing indicators
- Push notifications
- Production deployment guides (Render, managed Kafka)

## Project Background

This project was created as a personal learning exercise after a large-scale distributed systems course at **Universitat Pompeu Fabra**. It explores microservices, event-driven design, WebSockets, and polyglot persistence in a real-time chat domain.

Feedback and issues are welcome on GitHub.
