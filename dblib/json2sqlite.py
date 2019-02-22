import dblib.cds2sqlite as cds2sqlite
import dblib.books2sqlite as books2sqlite

def book(config):
    books2sqlite.main(config=config)
    return 0

def cd(config):
    cds2sqlite.main(config)
    return 0