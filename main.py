import uvicorn


def main():
    uvicorn.run("app:app", host="127.0.0.1", port=8001, reload=True, log_level="info")


if __name__ == "__main__":
    main()
