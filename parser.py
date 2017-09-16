# -*- coding: utf-8 -*-

import argparse
import datetime
import json
import multiprocessing
import sys
import time
import urlparse
from codecs import getwriter
from StringIO import StringIO
from xml.etree import ElementTree

from setproctitle import setproctitle

import arrow
import requests
import urllib3
from arrow.parser import ParserError
from bs4 import BeautifulSoup, NavigableString

RECONNECTION_ATTEMPTS = 10
DEFAULT_PARSING_DELAY = 0.5


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class InsufficientDataError(Exception):
    pass


def retrieve_url(url):
    attempts = 0

    while attempts <= RECONNECTION_ATTEMPTS:
        attempts += 1

        try:
            resp = requests.get(url, verify=False)

            if resp.status_code > 500 or resp.status_code == 403:
                break

            if resp.status_code != 200:
                continue

            return resp.content
        except requests.ConnectionError:
            # FIXME: Report this
            pass
        except requests.HTTPError as e:
            if e.response.status > 500 or e.response.status == 403:
                # FIXME: Fail on this
                break

    return None


def get_freshness_meter(soup):
    critic_score_div = soup.find('div', attrs={'class': ['critic-score', 'meter']})

    if not critic_score_div:
        return None

    score = soup.find('span', attrs={'class': ['meter-value', 'superPageFontColor']})
    if not score:
        return None

    try:
        return int(score.text.strip('%'))
    except ValueError:
        return None


def get_review_information(soup):
    result = {
        'rotten': None,
        'fresh': None,
        'reviews': None,
        'rating': None
    }

    critics_dev = soup.find('div', attrs={'id': 'all-critics-numbers'})
    if not critics_dev:
        return result

    spans = critics_dev.findChildren('div', attrs={'class': 'superPageFontColor'})
    values = {}

    for number in spans:
        children = filter(lambda v: v != '\n', list(number.children))

        if len(children) < 2:
            continue

        label = children[0].text.strip(': ')
        if isinstance(children[1], NavigableString):
            value = children[1].strip()
        else:
            value = children[1].text.strip()

        values[label] = value

    result['rating'] = values.get('Average Rating')

    if 'Rotten' in values:
        result['rotten'] = int(values['Rotten'])

    if 'Fresh' in values:
        result['fresh'] = int(values['Fresh'])

    if 'Reviews Counted' in values:
        result['reviews'] = int(values['Reviews Counted'])

    return result


def get_actualization_date(json_ld):
    formatted_time = json_ld.get('dateModified')

    if not formatted_time:
        formatted_time = json_ld.get('datePublished')

    if not formatted_time:
        formatted_time = json_ld.get('dateCreated')

    if formatted_time:
        try:
            moment = arrow.get(formatted_time)
            return moment.timestamp
        except ParserError:
            # FIXME: Report on this
            pass

    return None


def get_movie_info(soup):
    result = {}

    json_ld_text = soup.find('script', attrs={'id': 'jsonLdSchema'})
    if not json_ld_text:
        raise InsufficientDataError()

    json_ld = json.loads(json_ld_text.text)

    title = json_ld['name']

    if json_ld['@type'] == 'TVSeason':
        if 'partOfSeries' in json_ld:
            title = json_ld['partOfSeries']['name']
        result['season'] = '{0:02}'.format(json_ld['seasonNumber'])
        started_on = datetime.datetime.strptime(json_ld['startDate'], '%Y-%M-%d')
        result['year'] = started_on.year
    else:
        title_node = soup.find('h1', attrs={'data-type': 'title'})
        cleaned_title = title_node.text.strip()
        _, year = cleaned_title.rsplit(' ', 1)
        result['year'] = int(year.strip('()'))

    result['original_title'] = title
    result['actualization_date'] = get_actualization_date(json_ld)

    return result


def clean_up_result(d):
    return dict(
        filter(lambda (k, v): v not in ['', None], d.iteritems())
    )


def parse_entry(url, result):
    setproctitle('Retrieving %s' % result['rtomat_id'])
    content = retrieve_url(url)
    if not content:
        return None

    setproctitle('Parsing %s' % result['rtomat_id'])
    soup = BeautifulSoup(content, 'html.parser')

    result['percent'] = get_freshness_meter(soup)
    result.update(get_review_information(soup))
    result.update(get_movie_info(soup))

    return clean_up_result(result)


def is_suitable_for_parsing(url):
    info = urlparse.urlsplit(url)
    parts = filter(lambda v: v, info.path.split('/'))

    if not parts:
        return False

    return all([
        info.hostname == 'www.rottentomatoes.com',
        parts[0] in ('m', 'tv'),
        parts[-1] != 'pictures',
        parts[-1] != 'trailers'
    ])


def parse_sitemap(sitemap_url):
    xml_text = retrieve_url(sitemap_url)

    root = ElementTree.parse(StringIO(xml_text))
    for element in root.getiterator():
        if '}' in element.tag:
            tag = element.tag.split('}', 1)[1]
        else:
            tag = element.tag

        if tag == 'loc':
            yield element.text


def preprocess_url(url):
    result = {
        'url': url,
        'rtt': None,
        'rtomat_id': None,
    }

    url_info = urlparse.urlsplit(url)
    if url_info.path:
        parts = url_info.path.split('/')
        result['rtt'], result['rtomat_id'] = parts[1:3]

        if len(parts) > 3 and result['rtt'] == 'tv':
            result['season'] = parts[3][1:]

            result['url'] = 'https://www.rottentomatoes.com/tv/{0}/s{1}/'.format(
                result['rtomat_id'], result['season']
            )

    return result['url'], result


def format_traceback(exc, exc_traceback):
    filename = line = None

    while exc_traceback is not None:
        f = exc_traceback.tb_frame
        line = exc_traceback.tb_lineno
        filename = f.f_code.co_filename
        exc_traceback = exc_traceback.tb_next

    return json.dumps({
        'error': True,
        'details': {
            'message': str(exc),
            'file': filename,
            'line': line
        }
    }, ensure_ascii=False)


def parser_worker(request_q, reply_q, parsing_delay=DEFAULT_PARSING_DELAY):
    while True:
        setproctitle('Sleeping...')
        time.sleep(parsing_delay)

        setproctitle('Waiting for requests...')
        if request_queue.empty():
            continue

        request = request_q.get()
        if not request:
            break

        url, result = request

        try:
            result = parse_entry(url, result)
            reply_q.put(result)
        except Exception, e:
            setproctitle('Report %s' % e.message)
            reply_q.put(format_traceback(e, sys.exc_info()[2]))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description="Scrape review data from Rotten Tomatoes")
    arg_parser.add_argument('--only-movies', dest='allowed_types', action='store_const',
                        const=['m'], default=['m', 'tv'],
                        help='Scrape only movies.')
    arg_parser.add_argument('--only-tv', dest='allowed_types', action='store_const',
                        const=['tv'], default=['m', 'tv'],
                        help='Scrape only TV shows.')
    arg_parser.add_argument('--parse-delay', dest='parsing_delay', default=DEFAULT_PARSING_DELAY,
                        help='Pause between parsing attempts.', type=float)
    arg_parser.add_argument('--num-workers', dest='num_workers', default=4,
                        help='Number of workers.', type=int)
    arg_parser.add_argument('sitemaps', nargs='*', metavar='SITEMAP_URL',
                        type=str, default=['https://www.rottentomatoes.com/sitemap.xml'],
                        help='Starting points.')

    args = arg_parser.parse_args()

    sitemaps = args.sitemaps
    visited_pages = set()

    stdout = getwriter("utf8")(sys.stdout)
    stderr = getwriter("utf8")(sys.stderr)

    request_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    outstanding_requests = 0
    workers = [
        multiprocessing.Process(target=parser_worker, args=(request_queue, result_queue, float(args.parsing_delay)))
        for _ in xrange(args.num_workers)
    ]

    for worker in workers:
        worker.start()

    while sitemaps:
        sitemap_url = sitemaps.pop()

        for link in parse_sitemap(sitemap_url):
            if 'sitemap' in link and link.endswith('.xml'):
                sitemaps.insert(0, link)
                continue

            try:
                if is_suitable_for_parsing(link):
                    processed_url, result = preprocess_url(link)

                    if processed_url in visited_pages:
                        continue

                    if result['rtt'] not in args.allowed_types:
                        continue

                    visited_pages.add(processed_url)

                    outstanding_requests += 1
                    request_queue.put((processed_url, result))
            except Exception, e:
                exc_traceback = sys.exc_info()[2]
                sys.stderr.write(format_traceback(e, exc_traceback) + '\n')

        while not result_queue.empty() or outstanding_requests > 0:
            result = result_queue.get()

            if result:
                stdout.write(json.dumps(result, ensure_ascii=False))
                stdout.write('\n')
                stdout.flush()

            outstanding_requests -= 1

    request_queue.close()
    result_queue.close()

    for worker in workers:
        worker.terminate()
        worker.join()
