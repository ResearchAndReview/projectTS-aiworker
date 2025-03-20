import logging
from src import config

def main():
    config.load_config()
    logging.info("AI Task Distributor for projectTS initiated")
    # DO SOMETHING


if __name__ == "__main__":
    main()