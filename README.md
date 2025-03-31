# Key Value Cache

This repo lightweight and fast key-value cache for Python. It is designed to be simple and easy to use, with a focus on performance.

# How is it made

The server is a TCP server runnning on port 7171 that will listen for the `get` and `set` commands. The server can be started using the following command from the root of the project:

```bash
uv run server
```

The cache folder is a 