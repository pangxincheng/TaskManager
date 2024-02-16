
# empty frame
EMPTY = b''

# command
DISCONNECT = b'\001'
HEARTBEAT  = b'\002'
CALL       = b'\003'
REPLY      = b'\004'
REGISTER   = b'\005'
# DISCONNECT = b'DISCONNECT'
# HEARTBEAT  = b'HEARTBEAT'
# CALL       = b'CALL'
# REPLY      = b'REPLY'
# REGISTER   = b'REGISTER'

# others
SERVICE_SPLIT = '/'
SERVICE_NOT_FOUND = b'{"status":404,"msg":"Service not found"}'