import eosscitoken
cred = "/etc/xrootd/eos-pkey.pem"
key = "/etc/xrootd/eos-key.pem"
keyid = "eos"
issuer = "localhost"
factory_instance = eosscitoken.c_scitoken_factory_init(cred, key, keyid, issuer)
token_length = 4096
token_buffer = bytearray(token_length)
result = eosscitoken.c_scitoken_create(token_buffer, token_length, 86400,"scope=storage.read:\"/eos/alice/grid/01/16384/18d1b39d-0124-4517-898d-81547ddf1016\"","","")
print(result)
print(token_buffer.split(b'\x00', 1)[0].decode('utf-8'))







