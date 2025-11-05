import hashlib, random, time, struct

# CONSTANTS
aa = [
    0xFFFFFFFF, 138, 1498001188, 211147047, 253, None, 203, 288, 9,
    1196819126, 3212677781, 135, 263, 193, 58, 18, 244, 2931180889, 240, 173,
    268, 2157053261, 261, 175, 14, 5, 171, 270, 156, 258, 13, 15, 3732962506,
    185, 169, 2, 6, 132, 162, 200, 3, 160, 217618912, 62, 2517678443, 44, 164,
    4, 96, 183, 2903579748, 3863347763, 119, 181, 10, 190, 8, 2654435769, 259,
    104, 230, 128, 2633865432, 225, 1, 257, 143, 179, 16, 600974999, 185100057,
    32, 188, 53, 2718276124, 177, 196, 4294967296, 147, 117, 17, 49, 7, 28, 12,
    266, 216, 11, 0, 45, 166, 247, 1451689750,
]
Ot = [aa[9], aa[69], aa[51], aa[92]]
MASK32 = 0xFFFFFFFF

# Helpers 
def u32(x): return x & MASK32
def rotl(x, n): return u32((x << n) | (x >> (32 - n)))

# ChaCha core 
def quarter(st, a, b, c, d):
    st[a] = u32(st[a] + st[b]); st[d] = rotl(st[d] ^ st[a], 16)
    st[c] = u32(st[c] + st[d]); st[b] = rotl(st[b] ^ st[c], 12)
    st[a] = u32(st[a] + st[b]); st[d] = rotl(st[d] ^ st[a],  8)
    st[c] = u32(st[c] + st[d]); st[b] = rotl(st[b] ^ st[c],  7)

def chacha_block(state, rounds):
    w = state[:]
    r = 0
    while r < rounds:
        # column
        quarter(w,0,4,8,12); quarter(w,1,5,9,13)
        quarter(w,2,6,10,14); quarter(w,3,7,11,15)
        r += 1
        if r >= rounds: break
        # diagonal
        quarter(w,0,5,10,15); quarter(w,1,6,11,12)
        quarter(w,2,7,12,13); quarter(w,3,4,13,14)
        r += 1
    return [(w[i] + state[i]) & MASK32 for i in range(16)]

def bump_counter(st): st[12] = u32(st[12] + 1)

# PRNG (faithful) 
def init_prng_state():
    now = int(time.time()*1000)
    return [
        aa[44], aa[74], aa[10], aa[62], aa[42], aa[17], aa[2], aa[21],
        aa[3], aa[70], aa[50], aa[32], aa[0] & now,
        random.randrange(aa[77]), random.randrange(aa[77]), random.randrange(aa[77])
    ]

kt = init_prng_state()
St = 0

def rand():
    global kt, St
    e = chacha_block(kt, 8)
    t = e[St]
    r = (e[St+8] & 0xFFFFFFF0) >> 11
    if St == 7:
        bump_counter(kt); St = 0
    else:
        St += 1
    return (t + 4294967296 * r) / 2**53

# Utilities
def num_to_bytes(val):
    if val < 255*255:
        return [(val>>8)&0xFF, val&0xFF]
    return [(val>>24)&0xFF,(val>>16)&0xFF,(val>>8)&0xFF,val&0xFF]

def be_int_from_str(s):
    b = s.encode()[:4]
    acc = 0
    for x in b: acc = (acc<<8)|x
    return acc & MASK32

# Encryption helpers 
def encrypt_chacha(key_words, rounds, data_bytes):
    nFull = len(data_bytes)//4
    leftover = len(data_bytes)%4
    words = []
    for i in range(nFull):
        j=4*i
        words.append(data_bytes[j]|(data_bytes[j+1]<<8)|(data_bytes[j+2]<<16)|(data_bytes[j+3]<<24))
    if leftover:
        v=0; base=4*nFull
        for c in range(leftover): v |= data_bytes[base+c]<<(8*c)
        words.append(v)
    # xor
    o=0; state=key_words[:]
    while o+16 < len(words):
        stream = chacha_block(state, rounds); bump_counter(state)
        for k in range(16): words[o+k]^=stream[k]
        o+=16
    remain=len(words)-o
    stream=chacha_block(state, rounds)
    for k in range(remain): words[o+k]^=stream[k]
    # flatten
    out=[]
    for i,w in enumerate(words[:-1] if leftover else words):
        out+=[w&0xFF,(w>>8)&0xFF,(w>>16)&0xFF,(w>>24)&0xFF]
    if leftover:
        w=words[-1]; base=4*nFull
        for c in range(leftover): out.append((w>>(8*c))&0xFF)
    return out

def Ab22(key12Words, rounds, s):
    state = Ot + key12Words
    data = [ord(ch) for ch in s]
    enc = encrypt_chacha(state, rounds, data)
    return ''.join(map(chr,enc))

# Main encrypt API
def encrypt(queryString, body, userAgent, envcode=0, version="5.1.1", timestampMs=None):
    if timestampMs is None: timestampMs = int(time.time()*1000)

    obj = {}
    obj[1]=1
    obj[2]=envcode
    obj[3]=hashlib.md5(queryString.encode()).hexdigest()
    obj[4]=hashlib.md5(body.encode()).hexdigest()
    obj[5]=hashlib.md5(userAgent.encode()).hexdigest()
    obj[6]=timestampMs//1000
    obj[7]=1508145731
    obj[8]=(timestampMs*1000)%2147483648
    obj[9]=version

    if version=="5.1.1":
        obj[10]="1.0.0.314"; obj[11]=1
        v12=0
        for i in range(1,12):
            v=obj[i]; toXor=v if isinstance(v,int) else be_int_from_str(v)
            v12 ^= toXor
        obj[12]=v12 & MASK32
    elif version!="5.1.0":
        raise Exception("Unsupported version")

    v0=0
    for i in range(1,len(obj)+1):
        v=obj[i]
        if isinstance(v,int): v0^=v
    obj[0]=v0 & MASK32

    payload=[len(obj)]
    for k,v in obj.items():
        payload.append(k)
        if isinstance(v,int):
            valBytes=num_to_bytes(v)
        else:
            valBytes=list(v.encode())
        payload+=num_to_bytes(len(valBytes))
        payload+=valBytes
    baseStr=''.join(map(chr,payload))

    keyWords=[]; keyBytes=[]; roundAccum=0
    for i in range(12):
        rnd=rand()
        word=int(rnd*4294967296)&MASK32
        keyWords.append(word)
        roundAccum=(roundAccum+(word&15))&15
        keyBytes+=[word&0xFF,(word>>8)&0xFF,(word>>16)&0xFF,(word>>24)&0xFF]
    rounds=roundAccum+5

    enc=Ab22(keyWords,rounds,baseStr)

    insertPos=0
    for b in keyBytes: insertPos=(insertPos+b)%(len(enc)+1)
    for ch in enc: insertPos=(insertPos+ord(ch))%(len(enc)+1)

    keyBytesStr=''.join(map(chr,keyBytes))
    finalStr=chr(((1<<6)^(1<<3)^3)&0xFF)+enc[:insertPos]+keyBytesStr+enc[insertPos:]

    alphabet="u09tbS3UvgDEe6r-ZVMXzLpsAohTn7mdINQlW412GqBjfYiyk8JORCF5/xKHwacP="
    out=[]
    fullLen=(len(finalStr)//3)*3
    for i in range(0,fullLen,3):
        block=(ord(finalStr[i])<<16)|(ord(finalStr[i+1])<<8)|ord(finalStr[i+2])
        out+=[alphabet[(block>>18)&63],alphabet[(block>>12)&63],
              alphabet[(block>>6)&63],alphabet[block&63]]
    return ''.join(out)