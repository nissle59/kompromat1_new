from bs4 import BeautifulSoup, Comment, Tag
from urllib.parse import urlparse
import base64
from sql import *
from scraper import GET
from pathlib import *


log = logging.getLogger("parser")
log_level = logging.INFO


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
        if img_src[:4] != 'data':
            if img_src[:2] == '//':
                img_src = 'https:' + img['src']
            rblob = GET(img_src)
            if rblob:
                blob = rblob.content
                img_b64 = base64.b64encode(blob).decode()
                img_src = 'data:image/png;base64,' + img_b64
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
    if_count = 1
    iframes = []
    for iframe in article.find_all('iframe'):
        if iframe['src'][:2] == '//':
            iframe_src = 'https:' + iframe['src']
        else:
            iframe_src = iframe['src']
        iframe.attrs = {}
        iframe.name = 'a'
        iframe['href'] = iframe_src
        iframe['target'] = '_blank'
        iframe.string.replace_with(f"| Источник №{if_count} |")
        if_count += 1
        iframes.append(iframe.extract())

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
        'post':post
    }


def parse_article(file_json, date):
    _log = logging.getLogger('parser.parse_article')
    art_catalog = Path(file_json).parent
    html_file = list(art_catalog.rglob('*.html'))
    d_file = json.loads(Path(file_json).read_text(encoding='utf-8'))
    url = d_file['source']
    d = None
    art = {}
    img = None
    if html_file:
        html_file = html_file[0]
        local_id = int(urlparse(url).path.split('/')[-1:][0].split('-')[0])
        origin = f'{urlparse(url).scheme}://{urlparse(url).netloc}/'
        html = html_file.read_text(encoding='utf-8')
        art = clear_article(url, html)

        d = {
            'local_id': local_id,
            'name': art['title'],
            'origin': origin,
            'source': url,
            'date': date,
            'description': art['post'],
        }

    if d:
        if sql_add_article(d):
            config.CURRENT_LINK += 1
            sql_set_link_downloaded(d['source'])
            title_img = list(art_catalog.rglob('title_img.*'))
            if title_img:
                title_img = title_img[0]
                try:
                    image = {
                        'source': url,
                        'b_data': title_img.read_bytes(),
                        'ext': title_img.suffix
                    }
                    sql_add_image(image)
                except Exception as e:
                    _log.info(e)
            _log.info(
                f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {url} parsed and added')
        else:
            _log.info(f'{url} parsed, NOT added')
    else:
        _log.info(f'{url} FAILED')



if __name__ == '__main__':
    init_logs()
    init_db(config.SSH_TUNNELED)

    links = sql_get_links()

    close_db(config.SSH_TUNNELED)