import os


def build_kafka_config(*, client_id: str | None = None, group_id: str | None = None) -> dict:
    """Local Docker Kafka by default; Confluent Cloud when SASL env vars are set."""
    config: dict = {
        "bootstrap.servers": os.getenv("KAFKA_BROKER", "kafka-1:9092"),
    }

    if client_id:
        config["client.id"] = client_id
    if group_id:
        config["group.id"] = group_id
        config["auto.offset.reset"] = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

    username = os.getenv("KAFKA_SASL_USERNAME") or os.getenv("KAFKA_API_KEY")
    password = os.getenv("KAFKA_SASL_PASSWORD") or os.getenv("KAFKA_API_SECRET")

    if username and password:
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": username,
                "sasl.password": password,
            }
        )

    return config
