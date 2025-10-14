import hashlib
import base64
import time

STANDARD_B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
CUSTOM_B64_ALPHABET   = "Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe"

# Mapping chuẩn -> custom
ENC_TRANS = {STANDARD_B64_ALPHABET[i]: CUSTOM_B64_ALPHABET[i] for i in range(len(STANDARD_B64_ALPHABET))}

def custom_b64_encode(data: bytes) -> str:
    b64 = base64.b64encode(data).decode()
    return "".join(ENC_TRANS.get(ch, ch) for ch in b64)

def std_md5_enc(data: bytes) -> bytes:
    return hashlib.md5(data).digest()

def rc4_enc(key: bytes, plaintext: bytes) -> bytes:
    s = list(range(256))
    j = 0
    key_len = len(key)

    # KSA
    for i in range(256):
        j = (j + s[i] + key[i % key_len]) & 0xff
        s[i], s[j] = s[j], s[i]

    # PRGA
    i = j = 0
    out = bytearray(len(plaintext))
    for n in range(len(plaintext)):
        i = (i + 1) & 0xff
        j = (j + s[i]) & 0xff
        s[i], s[j] = s[j], s[i]
        k = s[(s[i] + s[j]) & 0xff]
        out[n] = plaintext[n] ^ k
    return bytes(out)

def xor_key(buf: bytes) -> int:
    x = 0
    for b in buf:
        x ^= b
    return x

def encrypt(params: str, postData: str, userAgent: str, timestamp: int) -> str:
    uaKey   = bytes([0x00, 0x01, 0x0e])
    listKey = bytes([0xff])
    fixedVal = 0x4a41279f  # 3845494467

    # double-MD5
    md5Params = std_md5_enc(std_md5_enc(params.encode("utf-8")))
    md5Post   = std_md5_enc(std_md5_enc(postData.encode("utf-8")))

    # UA -> RC4 -> Base64 -> MD5
    uaRc4 = rc4_enc(uaKey, userAgent.encode("utf-8"))
    uaB64 = base64.b64encode(uaRc4).decode()
    md5Ua = std_md5_enc(uaB64.encode("ascii"))

    # build buffer (18 bytes)
    parts = [
        bytes([0x40]),
        uaKey,
        md5Params[14:16],
        md5Post[14:16],
        md5Ua[14:16],
        (timestamp & 0xffffffff).to_bytes(4, "big"),
        (fixedVal & 0xffffffff).to_bytes(4, "big"),
    ]
    buffer = b"".join(parts)

    # checksum (append -> 19 bytes)
    checksum = xor_key(buffer)
    buffer += bytes([checksum])

    # final wrapper
    enc = bytes([0x02]) + listKey + rc4_enc(listKey, buffer)

    return custom_b64_encode(enc)
