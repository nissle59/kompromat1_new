import json
with open('config.json','r') as f:
    c = json.loads(f.read())

DEV = c['DEV']['DEV']
if DEV:
    DEV_LIMIT = c['DEV']['DEV_LIMIT']
else:
    DEV_LIMIT = 1

SSH_TUNNELED = c['SSH_TUNNELED']
MULTITHREADED = c['MULTITHREADED']
THREADS = c['THREADS']

proxies = c['proxies']
headers = c['headers']

base_url = c['base_url']
archive_url = base_url + c['archive_url']


class DB:
    db_user : str
    db_password : str
    db_host : str
    db_port : int
    db_name : str
    ssh_host : str
    ssh_port : int
    ssh_user : str
    ssh_password : str

    def __init__(self):
        db = c['db']
        ssh = c['ssh']
        self.db_host = db['host']
        self.db_port = db['port']
        self.db_user = db['user']
        self.db_password = db['pass']
        self.db_name = db['name']

        self.ssh_host = ssh['host']
        self.ssh_port = ssh['port']
        self.ssh_user = ssh['user']
        self.ssh_password = ssh['pass']

# DEV = False
# DEV_LIMIT = 500
# SSH_TUNNELED = False
#
# MULTITHREADED = True
# THREADS = 50
#
# proxies = [
#     'SOGBee:d25Hs5A@188.191.164.19:9078',
#     'SOGMeg:rTd57fsDh@188.191.164.19:9005',
#     'SOGMTS:gF56k2S@goldproxy2.linkpc.net:1109'
# ]
#
# headers = {
#     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
#     'accept-language': 'ru,en;q=0.9',
#     'cache-control': 'max-age=0',
#     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 YaBrowser/23.1.5.708 Yowser/2.5 Safari/537.36'
# }
#
# base_url = 'https://compromat.group/'
#

# class DB:
#     db_user = 'twpguser'
#     db_password = '785564tw'
#     db_host = 'localhost'
#     db_port = 5432
#     db_name = 'twdb'
#     ssh_host = 'pve.thron.host'
#     ssh_port = 7222
#     ssh_user = 'root'
#     ssh_password = 'qAzWsX159$$$'
#     def __init__(self):


TOTAL_LINKS = 0
CURRENT_LINK = 0
archive_links = []

iter_proxy = 0

REMOVE_ATTRIBUTES = ['lang','language','onmouseover','onmouseout','script','style','font',
                        'dir','face','size','color','style','class','width','height','hspace',
                        'border','valign','align','background','bgcolor','text','link','vlink',
                        'alink','cellpadding','cellspacing']