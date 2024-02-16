import uuid

def get_uuid() -> str:
    return str(uuid.uuid4())

def to_bytes(size: int, unit: str) -> int:
    if unit == "B":
        return int(size)
    if unit == "KiB":
        return int(size * 1024)
    if unit == "MiB":
        return int(size * 1024 * 1024)
    if unit == "GiB":
        return int(size * 1024 * 1024 * 1024)
    raise ValueError("Invalid unit")

def fmt_size(size):
    if size < 1024:
        return str(size) + " B"
    size /= 1024
    if size < 1024:
        return str(size) + " KiB"
    size /= 1024
    if size < 1024:
        return str(size) + " MiB"
    size /= 1024
    return str(size) + " GiB"

