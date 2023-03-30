import datetime
import json
import logging

import config
from config import DB, DEV, DEV_LIMIT, TOTAL_LINKS
import psycopg2
from psycopg2.extras import DictCursor
from sshtunnel import SSHTunnelForwarder

tunnel = None
sql_conn = None
sql_cur = None

log = logging.getLogger("parser")


def init_db(tunneled=True):
    global tunnel
    global sql_conn
    global sql_cur
    base = DB()
    if tunneled:
        tunnel = SSHTunnelForwarder(
            (base.ssh_host, base.ssh_port),
            ssh_username=base.ssh_user,
            ssh_password=base.ssh_password,
            remote_bind_address=(base.db_host, base.db_port)
        )
        tunnel.start()
        sql_conn = psycopg2.connect(
            user=base.db_user,
            password=base.db_password,
            host=base.db_host,
            port=tunnel.local_bind_port,
            database=base.db_name,
        )
    else:
        sql_conn = psycopg2.connect(
            user=base.db_user,
            password=base.db_password,
            host=base.db_host,
            port=base.db_port,
            database=base.db_name,
        )

    sql_cur = sql_conn.cursor(cursor_factory=DictCursor)


def close_db(tunneled=True):
    _log = logging.getLogger("parser.sql.destructor")
    global tunnel
    global sql_conn
    global sql_cur
    if sql_cur:
        sql_cur.close()
    else:
        _log.info('Create cursor before closing!')
    if sql_conn:
        sql_conn.close()
    else:
        _log.info('Create connection before closing!')
    if tunneled and tunnel:
        tunnel.close()
    else:
        _log.info('Create SSH tunnel before closing!')


def sql_get_last_link_date():
    _log = logging.getLogger('parser.sql.lastdate')
    q = "select date from links where link like %s and date is not null and downloaded = true order by date desc limit 1"
    try:
        sql_cur.execute(q, (config.base_url + '%',))
        record = sql_cur.fetchall()[0][0]
        rec_str = record.strftime("%Y-%m-%d")
        _log.info(f'Last date of articles: {str(rec_str)}')
        return str(rec_str)
    except:
        return None


def sql_push_link(lnk):
    _log = logging.getLogger('parser.sql.pushlink')
    if config.TOTAL_LINKS == 0:
        config.TOTAL_LINKS = -1
    try:
        if lnk['date']:
            insert_query = "INSERT INTO links (name, link, date) VALUES (%s, %s, %s)"
            sql_cur.execute(insert_query, (lnk['name'], lnk['link'], lnk['date']))
        else:
            insert_query = "INSERT INTO links (name, link) VALUES (%s, %s)"
            sql_cur.execute(insert_query, (lnk['name'], lnk['link']))
        sql_conn.commit()
        _log.info(
            f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] ({config.CURRENT_LINK} of {config.TOTAL_LINKS}) [{lnk["name"]}] Ok!')
        config.CURRENT_LINK += 1
        return True
    except Exception as e:
        _log.error(
            f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] ({config.CURRENT_LINK} of {config.TOTAL_LINKS}) [{lnk["name"]}] Record already exists')
        config.CURRENT_LINK += 1
        sql_conn.rollback()
        return False


def sql_push_links(lnks: list):
    def push_link(lnk):
        try:
            insert_query = "INSERT INTO links (name, link, date) VALUES (%s, %s, %s)"
            sql_cur.execute(insert_query, (lnk['name'], lnk['link'], lnk['date']))
            sql_conn.commit()
            return True
        except:
            return False

    _log = logging.getLogger('parser.sql.pushlinks')
    values = []
    try:
        insert_query = "INSERT INTO links (name, link, date) VALUES (%s, %s, %s)"
        for lnk in lnks:
            values.append((lnk['name'], lnk['link'], lnk['date']))
        sql_cur.executemany(insert_query, values)
        sql_conn.commit()
    except Exception as e:
        _log.debug(e)
        sql_conn.rollback()
        for lnk in lnks:
            if push_link(lnk):
                _log.info('Ok!')
            else:
                _log.info('Not Ok =(')


def sql_get_links():
    _log = logging.getLogger('parser.sql.get_links')
    select_query = "SELECT * FROM links WHERE downloaded = False AND link LIKE %s"  # AND uploaded = False"
    if DEV:
        select_query = f"SELECT * FROM links WHERE downloaded = False AND link LIKE %s LIMIT {DEV_LIMIT}"  # AND uploaded = False LIMIT 50"
        _log.info(f'DEV mode! with DEV_LIMIT = {DEV_LIMIT}')
    try:
        sql_cur.execute(select_query, (config.base_url + "%",))
        records = sql_cur.fetchall()
    except:
        sql_conn.rollback()
        sql_cur.execute(select_query, (config.base_url + "%",))
        records = sql_cur.fetchall()
    config.TOTAL_LINKS = len(records)
    print(config.TOTAL_LINKS)
    if config.TOTAL_LINKS > 0:
        return records
    else:
        return None


def sql_set_link_downloaded(link):
    _log = logging.getLogger('parser.sql.set_link_downloaded')
    try:
        q = "UPDATE links SET downloaded = %s WHERE link = %s"
        sql_cur.execute(q, (True, link))
        sql_conn.commit()
        return True
    except Exception as e:
        _log.error(e)
        sql_conn.rollback()
        return False


def sql_add_article(d: dict):
    _log = logging.getLogger('parser.sql.add_article')
    try:
        if ('date' in d.keys()) and ('tags' in d.keys()):
            q = "INSERT INTO articles (local_id, name, origin, source, date, description, tags) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (d['local_id'], d['name'], d['origin'], d['source'], d['date'], d['description'], d['tags'])
        elif ('date' in d.keys()) and ('tags' not in d.keys()):
            q = "INSERT INTO articles (local_id, name, origin, source, date, description) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (d['local_id'], d['name'], d['origin'], d['source'], d['date'], d['description'])
        elif ('date' not in d.keys()) and ('tags' in d.keys()):
            q = "INSERT INTO articles (local_id, name, origin, source, tags, description) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (d['local_id'], d['name'], d['origin'], d['source'], d['tags'], d['description'])
        elif ('date' not in d.keys()) and ('tags' not in d.keys()):
            q = "INSERT INTO articles (local_id, name, origin, source, description) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (d['local_id'], d['name'], d['origin'], d['source'], d['description'])
        sql_cur.execute(q, values)
        sql_conn.commit()
        if sql_set_link_downloaded(d['source']):
            return True
        else:
            return False
    except Exception as e:
        _log.error(e)
        # if sql_set_link_downloaded(d['source']):
        #     return True
        # else:
        #     return False
        sql_conn.rollback()
        return False


def sql_add_image(img: dict):
    _log = logging.getLogger('parser.sql.add_image')
    try:
        q = "INSERT INTO images (source, b_data, ext) VALUES (%s, %s, %s)"
        values = (img['source'], img['b_data'], img['ext'])
        sql_cur.execute(q, values)
        sql_conn.commit()
        return True
    except Exception as e:
        _log.debug(f'[NORMAL] image for {img["source"]} already exists')
        sql_conn.rollback()
        return False


def sql_dups_delete(table_name='articles', column_name='source'):
    _log = logging.getLogger('parser.sql.dups_delete')
    try:
        q = f"""DELETE FROM {table_name} a USING (
          SELECT MIN(ctid) as ctid, {column_name}
            FROM {table_name} 
            GROUP BY {column_name} HAVING COUNT(*) > 1
          ) b
          WHERE a.{column_name} = b.{column_name} 
          AND a.ctid <> b.ctid"""
        sql_cur.execute(q)
        sql_conn.commit()
        _log.info(f'Deleted duplicates from {table_name}')
        return True
    except Exception as e:
        _log.error(e)
        sql_conn.rollback()
        return False


def sql_version():
    _log = logging.getLogger('parser.sql')
    q = "SELECT version();"
    sql_cur.execute(q)
    _log.info(f'Version: {sql_cur.fetchone()[0]}')
