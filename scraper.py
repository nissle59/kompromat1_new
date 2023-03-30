import random
import base64
import requests
import locale
import json
import datetime
from bs4 import BeautifulSoup, Comment, Tag
from tqdm.auto import trange
from tqdm import tqdm
import concurrent.futures as pool
import threading, time

import config
# from config import *
import logging
from sql import *
from urllib.parse import urlparse

locale.setlocale(locale.LC_TIME, "ru_RU")

log = logging.getLogger("parser")
# log_level = logging.INFO

rs = requests.session()
rs.headers = config.headers
rs.proxies = config.proxies[config.iter_proxy]

rs.verify = False


def GET(url, timeout=5):
    _log = logging.getLogger('parser.GET')

    def with_proxy(url, proxy, timeout=timeout):
        px = {
            'http': 'http://' + proxy,
            'https': 'http://' + proxy
        }
        try:
            _log.debug(f'Try to {url} with proxy {px["https"]}')
            resp = rs.get(url, proxies=px, timeout=timeout)
            if resp.status_code in [200, 201]:
                return resp
            else:
                _log.debug(f'Failed with status {resp.status_code} - {url}')
        except:
            return None

    try:
        if not config.PROXIED:
            resp = rs.get(url, timeout=timeout)
            _log.debug(f'{resp.status_code}')
            if resp.status_code in [200, 201]:
                return resp
            else:
                random.SystemRandom().shuffle(config.proxies)
                for p in config.proxies:
                    try:
                        resp = with_proxy(url, p, timeout=timeout)
                        if resp.status_code in [200, 201]:
                            return resp
                        else:
                            _log.debug(f'Failed with status {resp.status_code} - {url}')
                    except Exception as e:
                        #_log.info(f'Failed with error {e}')
                        pass
        else:
            random.SystemRandom().shuffle(config.proxies)
            for p in config.proxies:
                try:
                    resp = with_proxy(url, p, timeout=timeout)
                    if resp.status_code in [200, 201]:
                        return resp
                    else:
                        _log.debug(f'Failed with status {resp.status_code} - {url}')
                except Exception as e:
                    # _log.info(f'Failed with error {e}')
                    pass
    except Exception as e:
        random.SystemRandom().shuffle(config.proxies)
        for p in config.proxies:
            try:
                resp = with_proxy(url, p, timeout=timeout)
                if resp.status_code in [200, 201]:
                    return resp
                else:
                    _log.debug(f'Failed with status {resp.status_code} - {url}')
            except Exception as e:
                pass
        _log.debug(f'{url} failed')
        return None


def get_archive_links():
    _log = logging.getLogger('parser.get_archive_links')
    links_urls = []
    path = config.archive_url[:-1]
    #logging.info(path)
    r = GET(path)
    if r:
        years = []
        html = r.content.decode('windows-1251')
        soup = BeautifulSoup(html, features="html.parser")
        base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
        h = base.find('h1')
        siblings = h.next_siblings
        for sibling in siblings:
            if repr(sibling) == '<hr/>':
                break
            if repr(sibling)[0] == '<':
                years.append(sibling.contents[0])

        for year in tqdm(years):
            path = config.archive_url + year
            r = GET(path)
            if r:
                html = r.content.decode('windows-1251')
                soup = BeautifulSoup(html, features="html.parser")
                base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
                months = base.find_all('div', {'class': 'month'})
                for month in months:
                    month_name = month.find('h4').contents[0].split(' ')[0].strip()
                    table = month.find('table')
                    links = table.find_all('a')
                    if len(links) > 0:
                        for link in links:
                            s = config.base_url[:-1] + link['href']

                            links_urls.append(s)
        config.archive_links = links_urls
        return links_urls
    else:
        _log.info(f'Can\'t get arch [{path}], try again')
        get_archive_links()


def _parse_page(html, url):
    _log = logging.getLogger('parser._parse_arch_page')
    soup = BeautifulSoup(html, features="html.parser")
    base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
    links_urls = []
    ls = base.find_all('a', {'class': 'articles_title'})
    for link in ls:
        d = {
            'name': link.text.strip(),
            'date': urlparse(url).path.split('/')[-1:][0],
            'link': config.base_url[:-1] + link['href']
        }
        #sql_push_link(d)
        links_urls.append(d)
    return links_urls


def get_day_links(url):
    _log = logging.getLogger('parser.get_day_links')
    path = url
    _log.info(f'--- page #1')
    r = GET(path)
    if r:
        html = r.content.decode('windows-1251')
        soup = BeautifulSoup(html, features="html.parser")
        base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
        pages = base.find('div', {'class': 'pagenate'})
        if pages:
            pages_count = int(pages.text.split(' ')[1].strip('():')) - 1
        else:
            pages_count = 0
        links_urls = []
        # links_urls = _parse_page(html)
        for page in range(1, pages_count):
            path = url + '?pg=' + str(page)
            _log.info(f'--- page #{page + 1}')
            r = GET(path)
            if r:
                html = r.content.decode('windows-1251')
                links_urls += _parse_page(html, url)
        return links_urls
    else:
        return None


def get_articles_links():
    _log = logging.getLogger('parser.get_articles_links')
    arch = get_archive_links()
    lnks = []
    cur_link = 1

    res_arr = []
    last_date = sql_get_last_link_date()
    #print(last_date)
    if last_date:
        for day in arch:
            art_dt = datetime.datetime.strptime(urlparse(day).path.split('/')[-1:][0],"%Y-%m-%d")
            if isinstance(last_date, str):
                last_dt = datetime.datetime.strptime(last_date,"%Y-%m-%d")
            else:
                last_dt = last_date
            time_diff = (last_dt - art_dt).days
            if time_diff <= 1:
                res_arr.append(day)
    else:
        res_arr = arch
    tot_links = len(res_arr)
    _log.info(f'{tot_links} days to parse...')

    for day in res_arr:
        _log.info(f'[{cur_link} of {tot_links}] LINK ({urlparse(day).path.split("/")[-1:][0]})')
        cur_link += 1
        ddd = None
        while not(ddd):
            ddd = get_day_links(day)
            if ddd is None:
                _log.info(f'trying to get page again!')
        lnks += ddd
    return lnks


def clear_article(url, html):
    def get_img_to_base64(img_src: str):
        if img_src.find('data:image') < 0:
            if img_src[:2] == '//':
                img_src = 'https:' + img['src']
            elif img_src[0] == '/':
                img_src = config.base_url[:-1] + img_src
            rblob = GET(img_src)
            if rblob:
                jpegs = ['jpg', 'jpeg']
                try:
                    ext = urlparse(img_src).path.split('/')[-1:][0].split('.')[-1:][0].lower()
                except:
                    ext = 'jpg'
                blob = rblob.content
                img_b64 = base64.b64encode(blob).decode()
                if ext in jpegs:
                    img_src = 'data:image/jpeg;base64,' + img_b64
                else:
                    img_src = 'data:image/png;base64,' + img_b64
                #img_src = 'data:image/png;base64,' + img_b64
                return img_src
            else:
                return None
        else:
            return img_src

    _log = logging.getLogger('parser.clear_article')
    soup = BeautifulSoup(html, features='html.parser')
    base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
    title = str(base.find('h1').contents[0])
    article = base.find('div', {'class': 'articles_one'})

    try:
        t_i_div = article.find('div', {'class': 'img_wrap'})
        t_i_div.extract()
        title_img_src = t_i_div.find('img')['src']
        r_img = GET(title_img_src)
        if r_img:
            ext = urlparse(title_img_src).path.split('/')[-1:][0].split('.')[-1:][0]
            title_img = {
                'source': url,
                'b_data': r_img.content,
                'ext': ext
            }
        else:
            title_img = None
            _log.info(f'Can\'t load title img ({title_img_src})')
    except:
        title_img = None

    tags = base.find_all('a', {'class': 'article-tag'})
    for tag in tags:
        tags[tags.index(tag)] = tag.text
    try:
        article.find('script').extract()
    except:
        pass
    try:
        video = article.find('video').extract()
    except:
        pass
    article.attrs = {}
    img_links = []
    for img in article.find_all('img'):
        # del img['width']
        img_res = get_img_to_base64(img['src'])
        if img_res:
            img.attrs = {}
            img['src'] = img_res
        else:
            img.extract()
        # img_links.append(img['src'])

    try:
        info = article.find('div', {'class': 'img_div'}).extract()
    except:
        pass

    for clr in article.find_all('div', {'class': 'clear'}):
        try:
            clr.extract()
        except:
            pass

    for div in article.find_all('div'):
        div.attrs = {}
        div.name = 'p'
        # del div['style']

    for a in article.find_all('a', {'class': 'link'}):
        a.replaceWithChildren()
    for p in article.find_all('p'):
        p.attrs = {}
    for em in article.find_all('em'):
        em.replaceWithChildren()
    for strong in article.find_all('strong'):
        strong.replaceWithChildren()

    for element in article(text=lambda text: isinstance(text, Comment)):
        element.extract()
    try:
        for element in article.find_all('br'):
            element.extract()
    except:
        pass

    if_count = 1
    iframes = []
    for iframe in article.find_all('iframe'):
        if iframe:
            try:
                if iframe['src'][:2] == '//':
                    iframe_src = 'https:' + iframe['src']
                else:
                    iframe_src = iframe['src']
                a = BeautifulSoup(f'<a target="_blank">| Источник №{if_count} |</a>',
                                  features="html.parser")
                a.a['href'] = iframe_src.replace('\"', '').replace('\\', '')
                iframes.append(a.a)
                if_count += 1
            except:
                pass
        try:
            iframe.extract()
        except:
            pass

    v = soup.find_all('div')
    for div in v:
        div.unwrap()
    v = soup.find_all('span')
    for span in v:
        span.unwrap()

    if len(iframes) > 0:
        pend = BeautifulSoup('<p></p>', features="html.parser")
        for iframe in iframes:
            pend.append(iframe)
    else:
        pend = None

    post = article.prettify().replace('<div>', '').replace('</div>', '').strip(' \n')
    if pend:
        post += pend.prettify().strip(' \n')

    return {
        'title':title,
        'img':title_img,
        'tags':"|".join(tags),
        'post':post
    }


def parse_article(url, date):
    _log = logging.getLogger('parser.parsearticle')
    resp = GET(url)
    url_short = urlparse(url).path.split("/")[-1:][0]
    d = None
    img = None
    art = None
    if resp:
        local_id = int(urlparse(url).path.split('/')[-1:][0].split('-')[0])
        origin = f'{urlparse(url).scheme}://{urlparse(url).netloc}/'
        html = resp.text
        art = clear_article(url, html)

        d = {
            'local_id': local_id,
            'name': art['title'],
            'origin': origin,
            'source': url,
            #'date': date,
            #'tags': art['tags'],
            'description': art['post']
        }
        if art['tags']:
            d.update({'tags':art['tags']})
        if date:
            d.update({'date':date})

    if d:
        if sql_add_article(d):
            sql_set_link_downloaded(d['source'])
            if art['img']:
                sql_add_image(art['img'])
            _log.info(
                f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- [SUCCESS] {url_short}')
        else:
            _log.info(f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- [NORMAL] {url_short} parsed, NOT added')
    else:
        _log.info(f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- [FAILED] {url_short}')


def parse_articles(links: dict):
    _log = logging.getLogger('parser.parse_articles')
    #urls = [link['link'] for link in links]
    for link in links:
        config.CURRENT_LINK += 1
        parse_article(link['link'],link['date'])



def multithreaded_parse_articles(links: dict):
    _log = logging.getLogger('parser.multiparse')
    t_s = []
    tc = config.THREADS

    l_count, l_mod = divmod(len(links), tc)

    l_mod = len(links) % tc

    if l_mod != 0:

        l_mod = len(links) % config.THREADS
        if l_mod == 0:
            tc = config.THREADS
            l_count = len(links) // tc

        else:
            tc = config.THREADS - 1
            l_count = len(links) // tc

    l_c = []
    for i in range(0, config.THREADS):
        _log.info(f'{i + 1} of {config.THREADS}')

        l_c.append(links[l_count * i:l_count * i + l_count])

    for i in range(0, config.THREADS):
        t_s.append(
            threading.Thread(target=parse_articles, args=(l_c[i],), daemon=True))
    for t in t_s:
        t.start()
        _log.info(f'Started thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')

    for t in t_s:
        t.join()
        _log.info(f'Joined thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')


if __name__ == "__main__":
    pass
