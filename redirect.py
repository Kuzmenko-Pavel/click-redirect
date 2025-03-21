#!/usr/bin/python
# This Python file uses the following encoding: utf-8
import Cookie
import base64
import datetime
import random
import sys
import re
import time
import urllib
import uuid
import urlparse
from wsgiref.simple_server import make_server

from trans import trans
from pymongo import MongoClient, errors

import tasks

sys.stdout = sys.stderr

BOTS_RE = re.compile('spider|pingdom|facebookexternalhit|phantomjs|casperjs|analyzer|daum|scanner|check|bot|okhttp|omgili|ltx71|curl|wget|newspaper|lipperhey|binlar|ahc|apache|proximic|embedly|http-client|preview|flipboard|parser|ips-agent|nutch|httrack|brandverity|fetch|httpunit|http_get|siteimprove|vkshare|siteexplorer|python|sentry|coccoc')

MONGO_HOST = 'srv-3.yottos.com:27017'


def redirect(environ, start_response):
    cid = str(uuid.uuid4())
    print "=" * 20
    print "START REDIRECT"
    cookie = ''

    def _redirect_to(redirect_url, cookie_id, cid):
        """ Перенаправление на ``url`` """
        url_parts = urlparse.urlparse(redirect_url)
        secure = ''
        if url_parts.scheme == 'https':
            secure = ' Secure;'
        uid = 'yottos_unique_id=' + cookie_id + '; Path=/; Version=1; Max-Age=31536000; Domain=.yottos.com; Secure; '
        cid = 'yottos_cid=' + cid + '; Path=/; Version=1; Max-Age=604800; Domain=' + url_parts.netloc + ';' + secure
        response_headers = [('Location', redirect_url), ('Set-Cookie', cid), ('Set-Cookie', uid)]
        start_response("302 Found", response_headers)
        return ""

    # Получаем словарь параметров
    try:
        bad_user = None
        elapsed_start_time = datetime.datetime.now()
        # Отделяем дополнительные GET-параметры от основных,
        # закодированных в base64
        status = environ['QUERY_STRING'].partition('&')[0]
        if status == 'status':
            start_response('200 OK', [('Content-type', 'text/plain')])
            return ""
        base64_encoded_params = environ['QUERY_STRING'].partition('&')[0]
        referer = environ.get('HTTP_REFERER', None)
        user_agent = environ.get('HTTP_USER_AGENT', None)
        param_lines = base64.urlsafe_b64decode(base64_encoded_params).splitlines()
        params = dict([(x.partition('=')[0], x.partition('=')[2]) for x in param_lines])
        print params
        url = params.get('url', 'https://yottos.com/')
        print "COOKIE ----"
        cookies = Cookie.SimpleCookie()
        cookies.load(environ.get('HTTP_COOKIE', ''))
        cook = cookies.get('yottos_unique_id')
        print cook
        if cook is not None:
            cookie = cook.value.strip()
        if cookie == '':
            cookie = str(int(time.time()))
            print environ
        print cookie
        print "COOKIE ----"
        print "REFERER ---"
        print referer
        if referer == 'None' or referer == '' or referer is None:
            referer = None
            print environ
        print "REFERER ---"
        print "HTTP_USER_AGENT ---"
        print user_agent
        if user_agent == 'None' or user_agent == '' or user_agent is None:
            user_agent = None
            print environ
        print "HTTP_USER_AGENT ---"

        # Проверяем действительность токена
        ip = environ['REMOTE_ADDR']
        offer_id = params.get('id', '')
        campaign_id = params.get('camp', '')
        inf_id = params.get('inf', '')
        token = params.get('token', '')
        print 'Token:', token
        valid = True if encrypt_decrypt(params.get('rand', ''), ip) == "valid" else False
        if not valid:
            bad_user = 'token'
        if user_agent and valid:
            if BOTS_RE.search(user_agent.lower()):
                print "!!!!!!! BOT !!!!!!!!!"
                bad_user = 'bt'
                valid = False
        if referer != None and 'yottos.com' not in referer:
            print "!!!!!!! FAKE REFERER !!!!!!!!!"
            bad_user = 'referer'
            valid = False
        redirect_datetime = datetime.datetime.now()
        #TODO параметр t depricated, в будушем удалить
        view_seconds = int(params.get('t', 0))
        if view_seconds == 0:
            view_seconds = (int(time.time()*1000) - int(params.get('tr', int(time.time()*1000))))
        view_seconds = view_seconds / 1000
        print "Valid click %s view_seconds in %s second" % (valid, view_seconds)
        if not valid:
            print "IP %s, cookie %s, token %s, offer id %s, validation -%s-" % (
                ip, cookie, token, offer_id, params.get('rand', ''))

        if _ban_check(ip):
            bad_user = 'bn'

        # Выделяем домен партнёра и добавляем его в целевой url
        # print "Выделяем домен партнёра и добавляем его в целевой url"
        url = utm_converter(url, offer_id, campaign_id, inf_id, cookie, bad_user, cid)
        # print "Create Task"
        try:
            tasks.process_click.delay(url=url,
                                      ip=ip,
                                      click_datetime=redirect_datetime,
                                      offer_id=offer_id,
                                      campaign_id=campaign_id,
                                      informer_id=inf_id,
                                      token=token,
                                      valid=valid,
                                      referer=referer,
                                      user_agent=user_agent,
                                      cookie=cookie,
                                      view_seconds=view_seconds,
                                      cid=cid)
        except Exception as ex:
            tasks.process_click(url=url,
                                ip=ip,
                                click_datetime=redirect_datetime,
                                offer_id=offer_id,
                                campaign_id=campaign_id,
                                informer_id=inf_id,
                                token=token,
                                valid=valid,
                                referer=referer,
                                user_agent=user_agent,
                                cookie=cookie,
                                view_seconds=view_seconds,
                                cid=cid)
            print ex
    except Exception as e:
        print e
        return _redirect_to(
            'https://yottos.com/?utm_source=yottos&utm_medium=redirect&utm_campaign=Not%20Valid%20Click', cookie, cid)
    print 'Redirect complit to %s ' % (datetime.datetime.now() - elapsed_start_time).microseconds
    print "=" * 20
    return _redirect_to(
        url or 'https://yottos.com/?utm_source=yottos&utm_medium=redirect&utm_campaign=Not%20Valid%20Click', cookie, cid)


def char_replace(string, chars=None, to_char=None):
    if chars is None:
        chars = [' ', '.', ',', ';', '!', '?']
    if to_char is None:
        to_char = '_'
    for ch in chars:
        if ch in string:
            string = string.replace(ch, to_char)
    return string


def utm_converter(url, offer_id, campaign_id, inf_id, cookie, bad_user, cid):
    offer_info = _get_offer_info(offer_id, campaign_id)
    partner_domain = _get_informer(inf_id)
    offer_title = 'yottos-' + offer_info['title'].encode('utf-8')
    offer_title = char_replace(offer_title)
    offer_campaign_title = 'yottos-' + offer_info['campaignTitle'].encode('utf-8')
    offer_campaign_title = char_replace(offer_campaign_title)
    offer_title_trans = urllib.quote(_ful_trans(offer_title))
    offer_campaign_title_trans = urllib.quote(_ful_trans(offer_campaign_title))
    if offer_info['marker'][1]:
        offer_title = offer_title_trans
        offer_campaign_title = offer_campaign_title_trans
    else:
        offer_title = urllib.quote(offer_title)
        offer_campaign_title = urllib.quote(offer_campaign_title)
    url = _add_dynamic_param(url, partner_domain, offer_campaign_title, offer_title, offer_info['marker'][2],
                             cookie, bad_user, cid)
    if offer_info['marker'][0]:
        url = _add_utm_param(url, type, partner_domain, offer_campaign_title, offer_title, offer_info['marker'][2],
                             offer_title_trans, offer_campaign_title_trans)
    print url
    return url


def encrypt_decrypt(word, ip):
    key = list(ip)
    output = []

    for i in range(len(word)):
        xor_num = ord(word[i]) ^ ord(key[i % len(key)])
        output.append(chr(xor_num))

    return ''.join(output)


def _u8(string):
    return unicode(string, 'utf-8')


def _eu8(string):
    return string.encode('utf-8')


def _ful_trans(string):
    f_trans = _eu8(_u8(string.replace(' ', '-')).encode('trans').lower())
    return f_trans


def _add_dynamic_param(url, source, campaign, name, hide, cookie, bad_user, cid):
    url_parts = list(urlparse.urlparse(url))

    params = dict(urlparse.parse_qsl(url_parts[3]))
    if len(params) > 0:
        for key, value in params.items():
            value = str(value)
            if hide:
                value = value.replace('{source}', source['guid'])
            else:
                value = value.replace('{source}', source['domain'])
            value = value.replace('{source_id}', source['guid_int'])
            value = value.replace('{source_guid}', source['guid'])
            value = value.replace('{campaign}', str(campaign))
            value = value.replace('{campaign_id}', str(campaign))
            value = value.replace('{campaign_guid}', str(campaign))
            value = value.replace('{name}', str(name))
            value = value.replace('{offer}', str(campaign))
            value = value.replace('{offer_id}', str(campaign))
            value = value.replace('{offer_guid}', str(campaign))
            value = value.replace('{rand}', str(random.randint(0, 1000000)))
            params[key] = value
        url_parts[3] = urllib.urlencode(params)

    query = dict(urlparse.parse_qsl(url_parts[4]))
    if len(query) > 0:
        for key, value in query.items():
            value = str(value)
            if hide:
                value = value.replace('{source}', source['guid'])
            else:
                value = value.replace('{source}', source['domain'])
            value = value.replace('{source_id}', source['guid_int'])
            value = value.replace('{source_guid}', source['guid'])
            value = value.replace('{campaign}', str(campaign))
            value = value.replace('{campaign_id}', str(campaign))
            value = value.replace('{campaign_guid}', str(campaign))
            value = value.replace('{name}', str(name))
            value = value.replace('{offer}', str(campaign))
            value = value.replace('{offer_id}', str(campaign))
            value = value.replace('{offer_guid}', str(campaign))
            value = value.replace('{rand}', str(random.randint(0, 1000000)))
            query[key] = value
    if 'yt_u_id' not in query:
        query.update({'yt_u_id': cookie})
    if 'yt_cid' not in query:
        query.update({'yt_cid': cid})
    if bad_user is None:
        query.update({'yt_u_g': 't'})
    else:
        query.update({'yt_u_b': bad_user})
    url_parts[4] = urllib.urlencode(query)
    return urlparse.urlunparse(url_parts)


def _add_utm_param(url, ad_type, source, campaign, name, hide, offer_title_trans, offer_campaign_title_trans):
    url_parts = list(urlparse.urlparse(url))

    query = dict(urlparse.parse_qsl(url_parts[4]))
    utm_medium = 'cpc_yottos'
    utm_source = source['domain']

    if ad_type == 'banner':
        utm_medium = 'cpm_yottos'

    if hide:
        utm_source = source['guid']

    utm_campaign = str(campaign)
    utm_content = str(name)
    utm_term = ''

    if 'utm_medium' not in query:
        query.update({'utm_medium': utm_medium})
    if 'utm_source' not in query:
        query.update({'utm_source': utm_source})
    else:
        utm_term = utm_source
    if 'utm_campaign' not in query:
        query.update({'utm_campaign': utm_campaign})
    if 'utm_content' not in query:
        query.update({'utm_content': utm_content})
    if 'utm_term' not in query:
        query.update({'utm_term': utm_term})
    if 'from' not in query:
        query.update({'from': 'Yottos'})

    # if '_openstat' not in query:
    #     query.update({'_openstat': ';'.join([utm_medium, offer_campaign_title_trans, offer_title_trans, utm_source])})
    url_parts[4] = urllib.urlencode(query)
    return urlparse.urlunparse(url_parts)


def _ban_check(ip):
    """ Проверяет в списке заблокированных """
    result = False
    try:
        db = MongoClient(MONGO_HOST).getmyad_db
        if db.blacklist.ip.find_one({'ip': ip}):
            result = True
    except (AttributeError, KeyError, errors.AutoReconnect) as e:
        print(e)
    return result


def _get_informer(informer_id):
    """ Возвращает домен, к которому относится информер ``informer_id`` """
    result = {'guid': 'None', 'guid_int': 'None', 'domain': 'None'}
    try:
        db = MongoClient(MONGO_HOST).getmyad_db
        inf = db.informer.find_one({'guid': informer_id})
        if inf:
            guid = inf.get('guid')
            guid_int = inf.get('guid_int')
            domain = inf.get('domain')
            domain = domain.replace('.', '_')
            result['guid'] = str(guid)
            result['guid_int'] = str(guid_int)
            result['domain'] = str(domain.encode('utf8'))
    except (AttributeError, KeyError, errors.AutoReconnect) as e:
        print(e)
    return result


def _get_offer_info(offer_id, campaign_id):
    """ Возвращает True, если к ссылке перехода на рекламное предложение
        ``offer_id`` необходимо добавить маркер yottos_partner=... """
    result = {'title': '', 'campaignTitle': '', 'marker': [True, False, False]}
    try:
        db = MongoClient(MONGO_HOST).getmyad_db
        offer = db.offer.find_one({'guid': offer_id}, ['title'])
        campaign = db.campaign.find_one({'guid': campaign_id}, ['title', 'yottosPartnerMarker', 'yottosTranslitMarker',
                                                                'yottosHideSiteMarker'])
        if offer and campaign:
            result['campaignTitle'] = campaign.get('title', 'NOT_TITLE')
            result['title'] = offer.get('title', 'NOT_TITLE')
            yottos_partner_marker = campaign.get('yottosPartnerMarker', True)
            yottos_translit_marker = True
            yottos_hide_site_marker = campaign.get('yottosHideSiteMarker', False)
            result['marker'] = [yottos_partner_marker, yottos_translit_marker, yottos_hide_site_marker]
    except (AttributeError, KeyError, errors.AutoReconnect) as e:
        print(e)
    return result


application = redirect

if __name__ == '__main__':
    httpd = make_server('', 8000, application)
    httpd.serve_forever()
