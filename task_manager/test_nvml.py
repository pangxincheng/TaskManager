import threading
import pynvml
pynvml.nvmlInit()

def fmt_size(size):
    if size < 1024:
        return str(size) + " B"
    size /= 1024
    if size < 1024:
        return str(size) + " KB"
    size /= 1024
    if size < 1024:
        return str(size) + " MB"
    size /= 1024
    return str(size) + " GB"

def get_info():
    pynvml.nvmlInit()
    deviceCount = pynvml.nvmlDeviceGetCount()
    for i in range(deviceCount):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        print("Device", i, ":", pynvml.nvmlDeviceGetName(handle))
        info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        print("Total memory:", fmt_size(info.total))
        print("Free memory:", fmt_size(info.free))
        print("Used memory:", fmt_size(info.used))
        print("Temperature:", pynvml.nvmlDeviceGetTemperature(handle, 0), "C")
        print("Power:", pynvml.nvmlDeviceGetPowerUsage(handle) / 1000, "W")
    pynvml.nvmlShutdown()
threading.Thread(target=get_info).start()
pynvml.nvmlShutdown()