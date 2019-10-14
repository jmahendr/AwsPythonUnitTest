#!C:\Users\JoshuaFrancis\PycharmProjects\DataDictionary\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'moto==1.3.13','console_scripts','moto_server'
__requires__ = 'moto==1.3.13'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('moto==1.3.13', 'console_scripts', 'moto_server')()
    )
