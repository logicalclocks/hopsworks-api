"""Allow ``python -m hopsworks.cli`` as an alternative to the ``hops`` script."""

from hopsworks.cli.main import main


if __name__ == "__main__":
    main()
