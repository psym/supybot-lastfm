import pickle, csv
import os, shutil
import gzip

class DictDB(dict):
    def __init__(self, filename, flag=None, mode=None, format=None, *args, **kwds):
        self.flag = flag or 'c'             # r=readonly, c=create, or n=new
        self.mode = mode                    # None or octal triple like 0x666
        self.format = format or 'csv'       # csv, or pickle
        self.filename = filename
        if flag != 'n' and os.access(filename, os.R_OK):
            file = gzip.open(filename, 'rb')
            try:
                self.load(file)
            finally:
                file.close()
        self.update(*args, **kwds)
    
    def sync(self):
        if self.flag == 'r':
            return
        filename = self.filename
        tempname = filename + '.tmp'
        file = gzip.open(tempname, 'wb')
        try:
            self.dump(file)
        except Exception:
            file.close()
            os.remove(tempname)
            raise
        file.close()
        shutil.move(tempname, self.filename)    # atomic commit
        if self.mode is not None:
            os.chmod(self.filename, self.mode)

    def close(self):
        self.sync()

    def dump(self, file):
        if self.format == 'csv':
            csv.writer(file).writerows(self.iteritems())
        elif self.format == 'pickle':
            pickle.dump(self.items(), file, -1)
        else:
            raise NotImplementedError('Unknown format: %r' % self.format)

    def load(self, file):
        # try formats from most restrictive to least restrictive
        for loader in (pickle.load, csv.reader):
            file.seek(0)
            try:
                return self.update(loader(file))
            except Exception, e:
                print e
                pass
        raise ValueError('File not in recognized format')


def dbopen(filename, flag=None, mode=None, format=None):
    return DictDB(filename, flag, mode, format)



if __name__ == '__main__':
    import random
    s = dbopen('tmp.shl', 'c', format='pickle')
    print(s, 'start')
    s['abc'] = '123'
    s['rand'] = random.randrange(10000)
    s.close()
    f = __builtins__.open('tmp.shl', 'rb')
    print (f.read())
    f.close()
