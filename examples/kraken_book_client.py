"""Run the independent consumer shipped with FeedTest.

Its implementation uses only stdlib and websockets, independently of the
replay engine and adapter validator.
"""

from feedtest.example_client import main, run  # noqa: F401

if __name__ == "__main__":
    main()
