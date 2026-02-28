"""XtremeFlow: A high-performance asynchronous task scheduler for LLM workloads."""

def __getattr__(name):
    if name == "__version__":
        from importlib.metadata import version, PackageNotFoundError

        try:
            val = version("xtremeflow")
        except PackageNotFoundError:
            val = "0.3.1"

        globals()["__version__"] = val
        return val

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
