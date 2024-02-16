import pycuda.driver as cuda
import pycuda.gpuarray as gpuarray

cuda.init()
print("%d device(s) found." % cuda.Device.count())
gpu_manager = []
for i in range(cuda.Device.count()):
    handler = cuda.Device(i)
    context = handler.make_context()
    gpu_manager.append({
        "handler": handler,
        "context": context,
        "chunks": [],
    })

for gpu in gpu_manager:
    gpu["context"].push()
    handler = gpu["handler"]
    gpu["chunks"].append(
        cuda.mem_alloc(1024 * 1024 * 1024)
    )
    cuda.Context.pop()