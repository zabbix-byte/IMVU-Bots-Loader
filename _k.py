import zipfile, marshal, dis, types

z = zipfile.ZipFile(r'C:\Users\VALERDAT\AppData\Roaming\IMVUClient\library.zip')
tmp = r'C:\Users\VALERDAT\AppData\Local\Temp\imvu_dis.pyo'


def load(name):
    open(tmp, 'wb').write(z.read(name))
    f = open(tmp, 'rb')
    f.read(8)
    co = marshal.load(f)
    f.close()
    return co


def walk(co, out):
    out.append(co)
    for c in co.co_consts:
        if isinstance(c, types.CodeType):
            walk(c, out)


mod = load('im/meet.pyo')
codes = []
walk(mod, codes)
for c in codes:
    if c.co_name == '_handleInvite':
        dis.dis(c)
