from bs4 import BeautifulSoup, Comment, Tag
from urllib.parse import urlparse
import base64
import warnings
import config
from sql import *
from scraper import GET
from pathlib import *
from tqdm import tqdm
import threading, time

warnings.filterwarnings("ignore")

log = logging.getLogger("parser")
log_level = logging.INFO
if config.LOGGING_DEBUG:
    log_level = logging.DEBUG


class MsgCounterHandler(logging.Handler):
    level2count = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.level2count = {}

    def emit(self, record):
        levelname = record.levelname
        if levelname not in self.level2count:
            self.level2count[levelname] = 0
        self.level2count[levelname] += 1


def init_logs(logname="parser"):
    """ Init logging to file and stdout
    """
    dt_fmt = "%Y%m%d %H%M%S"
    # out_fmt = "{asctime}|{levelname:<1}|{name}:{message}"
    out_fmt = "%(asctime)s|%(levelname).1s|%(name)s: %(message)s"
    formatter = logging.Formatter(out_fmt, dt_fmt)
    log.setLevel(log_level)
    dt_now = datetime.datetime.now().strftime("%Y-%m-%d")
    path = Path.cwd() / 'logs'
    path.mkdir(exist_ok=True)
    fname = f"{logname}_{dt_now}.log"
    file = path / fname
    fh = logging.FileHandler(file)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    mh = MsgCounterHandler()
    log.addHandler(mh)
    return


def clear_article(url, html) -> dict:
    def get_img_to_base64(img_src: str):
        #print(img_src)
        if img_src[-1:] == '/':
            img_src = img_src[:-1]
        if img_src.find('data:image') < 0:
            img_src = img_src.replace('\"', '').replace('\\', '')
            if img_src[:2] == '//':
                img_src = 'https:' + img['src']
            if img_src.count('//') > 1:
                img_src = 'https://' + img_src[len(config.base_url) + 1:]
            rblob = GET(img_src, timeout=1)
            if rblob:
                blob = rblob.content
                jpegs = ['jpg','jpeg']
                try:
                    ext = urlparse(img_src).path.split('/')[-1:][0].split('.')[-1:][0].lower()
                except:
                    ext = 'jpg'
                img_b64 = base64.b64encode(blob).decode()
                if ext in jpegs:
                    img_src = 'data:image/jpeg;base64,' + img_b64
                else:
                    img_src = 'data:image/png;base64,' + img_b64
                return img_src
            else:
                return None
        else:
            return img_src

    _log = logging.getLogger('parser.clear_article')
    soup = BeautifulSoup(html, features='html.parser')
    article = soup
    # article = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
    # title = str(base.find('h1').contents[0])
    # article = base.find('div', {'class': 'articles_one'})

    # try:
    #     t_i_div = article.find('div', {'class': 'img_wrap'})
    #     t_i_div.extract()
    #     title_img_src = t_i_div.find('img')['src']
    #     r_img = GET(title_img_src)
    #     if r_img:
    #         ext = urlparse(title_img_src).path.split('/')[-1:][0].split('.')[-1:][0]
    #         title_img = {
    #             'source': url,
    #             'b_data': r_img.content,
    #             'ext': ext
    #         }
    #     else:
    #         title_img = None
    #         _log.info(f'Can\'t load title img ({title_img_src})')
    # except:
    #     title_img = None

    # tags = base.find_all('a', {'class': 'article-tag'})
    # for tag in tags:
    #     tags[tags.index(tag)] = tag.text
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
        # 'title':title,
        'post': post.strip(' \r\n').replace('\n',' ')
    }


def parse_article(file_json, date=None):
    _log = logging.getLogger('parser.parse_article')
    art_catalog = Path(file_json).parent
    html_file = list(art_catalog.rglob('*.html'))
    d_file = json.loads(Path(file_json).read_text(encoding='utf-8'))
    url = d_file['source']
    d = None
    art = {}
    img = None
    if html_file:
        #print(url)
        html_file = html_file[0]
        local_id = int(urlparse(url).path.split('/')[-1:][0].split('-')[0])
        origin = f'{urlparse(url).scheme}://{urlparse(url).netloc}/'
        html = html_file.read_text(encoding='utf-8')
        art = clear_article(url, html)

        d = {
            'local_id': local_id,
            'name': d_file['name'],
            'origin': origin,
            'source': url,
            #    'date': date,
            'description': art['post'],
        }
        if date:
            d.update({'date': date})
        if d_file['tags']:
            d.update({'tags': "|".join(d_file['tags'])})
    else:
        _log.debug(f"No html file for {url}")
    if d:
        if sql_add_article(d):
            #config.CURRENT_LINK += 1
            sql_set_link_downloaded(d['source'])
            title_img = list(art_catalog.rglob('title_img.*'))
            if title_img:
                title_img = title_img[0]
                try:
                    image = {
                        'source': url,
                        'b_data': title_img.read_bytes(),
                        'ext': title_img.suffix[1:]
                    }
                    sql_add_image(image)
                except Exception as e:
                    _log.info(e)
            _log.info(
                f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {d_file["name"]} parsed and added')
        else:
            #config.CURRENT_LINK += 1
            _log.debug(f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {d_file["name"]} parsed, NOT added')
    else:
        #config.CURRENT_LINK += 1
        _log.debug(f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {d_file["name"]} FAILED')


def get_files_and_urls():
    _log = logging.getLogger('parser.prepare')
    _log.info('Getting URLs...')
    config.urls = [link['link'] for link in links]
    _log.info('Getting FILEs...')
    config.files = list(Path(Path.cwd() / 'pages').rglob('*/*.json'))
    _log.info('Done!')
    return {'files':config.files, 'urls': config.urls}


def parse_articles(files):
    _log = logging.getLogger('parser.parse_articles')
    # urls = [link['link'] for link in links]
    # files = list(Path(Path.cwd() / 'pages').rglob('*/*.json'))
    for file in files:
        config.CURRENT_LINK += 1
        #print(file)
        js = json.loads(Path(file).read_text())
        if js['source'] in config.urls:
            d = parse_article(Path(file), js['date'])
        else:
            _log.debug(f"{js['source']} not in SQL LINKS")
    _log.info(f'Parsing ended normally')


def get_all_links(init_catalog=Path(Path.cwd() / 'pages')):
    all_jsons = list(init_catalog.rglob('*/*.json'))
    links = []
    for file in tqdm(all_jsons):
        d = json.loads(Path(file).read_text(encoding='utf-8'))
        lnk = {
            'link': d['source'],
            'name': d['name']
        }
        if 'date' in d.keys():
            lnk.update({
                'date': d['date']
            })
        links.append(lnk)
    return links


def multithreaded_parse_articles():
    _log = logging.getLogger('parser.multiparse')
    t_s = []
    tc = config.THREADS
    files = config.files
    l_count, l_mod = divmod(len(files), tc)

    l_mod = len(files) % tc

    if l_mod != 0:

        l_mod = len(files) % config.THREADS
        if l_mod == 0:
            tc = config.THREADS
            l_count = len(files) // tc

        else:
            tc = config.THREADS - 1
            l_count = len(files) // tc

    l_c = []
    for i in range(0, config.THREADS):
        _log.info(f'{i + 1} of {config.THREADS}')

        l_c.append(files[l_count * i:l_count * i + l_count])

    for i in range(0, config.THREADS):
        t_s.append(
            threading.Thread(target=parse_articles, args=(l_c[i],), daemon=True))
    for t in t_s:
        t.start()
        _log.info(f'Started thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} files')

    for t in t_s:
        t.join()
        _log.info(f'Joined thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} files')



if __name__ == '__main__':
    init_logs()
    init_db(config.SSH_TUNNELED)
    sql_version()
    sql_dups_delete()

    # lnks = get_all_links()
    # config.TOTAL_LINKS = len(lnks)
    # for lnk in lnks:
    #     sql_push_link(lnk)

    links = sql_get_links()
    config.CURRENT_LINK = 0
    if links:
        get_files_and_urls()
        if config.MULTITHREADED:
            multithreaded_parse_articles()
        else:
            parse_articles()
    else:
        log.info('No articles to parse')

    sql_dups_delete()
    close_db(config.SSH_TUNNELED)
