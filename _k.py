import zipfile

z = zipfile.ZipFile(r'C:\Users\VALERDAT\AppData\Roaming\IMVUClient\library.zip')
d = z.read('imvu/client/sessionwindow.pyo')
i = d.find(b'*msg Seat')
seg = d[i:i+28]
print('seat template ords:', [ord(b) for b in seg])
j = d.find(b'*putOnOutfit')
print('outfit ords:', [ord(b) for b in d[j:j+16]])
