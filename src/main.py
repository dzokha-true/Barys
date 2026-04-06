from config import Config
from deps import init_services
from cli import CLI


def main() -> None:
    config = Config()
    services = init_services(config)

    shell = CLI(
        query_service=services.query_service,
        ingestor=services.csv_ingestor,
    )

    try:
        shell.cmdloop()
    finally:
        services.schema_manager.close()
        services.query_executor.close()


if __name__ == "__main__":
    main()
