#!/usr/bin/python
# This Python file uses the following encoding: utf-8
import Cookie
import base64
import datetime
import time
import unittest
import urlparse
import tasks
import sys
import pymongo
import urllib
import random
import re
from wsgiref.simple_server import make_server
from trans import trans

sys.stdout = sys.stderr

MONGO_HOST = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


def redirect(environ, start_response):
    print "=" * 20
    print "START REDIRECT"
    cookie = ''

    def _redirect_to(url, cookie):
        ''' Перенаправление на ``url`` '''
        response_headers = [('Location', url),
                            ('set-cookie',
                             'yottos_unique_id=' + cookie + '; Path=/; Version=1; Max-Age=31536000; Domain=.yottos.com; HttpOnly;')]
        start_response("302 Found", response_headers)
        return ""

    # Получаем словарь параметров
    try:
        elapsed_start_time = datetime.datetime.now()
        # Отделяем дополнительные GET-параметры от основных,
        # закодированных в base64
        status = environ['QUERY_STRING'].partition('&')[0]
        if status == 'status':
            start_response('200 OK', [('Content-type', 'text/plain')])
            return ""
        base64_encoded_params = environ['QUERY_STRING'].partition('&')[0]
        referer = environ.get('HTTP_REFERER', 'None')
        user_agent = environ.get('HTTP_USER_AGENT', 'None')
        param_lines = base64.urlsafe_b64decode(base64_encoded_params).splitlines()
        params = dict([(x.partition('=')[0], x.partition('=')[2]) for x in param_lines])
        print params
        url = params.get('url', 'https://yottos.com/')
        print "COOKIE ----"
        cookies = Cookie.SimpleCookie()
        cookies.load(environ.get('HTTP_COOKIE',''))
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
        if referer == 'None' or referer == '' or referer == None:
            print environ
        print "REFERER ---"
        print "HTTP_USER_AGENT ---"
        print user_agent
        print "HTTP_USER_AGENT ---"

        # Проверяем действительность токена
        ip = environ['REMOTE_ADDR']
        offer_id = params.get('id', '')
        campaign_id = params.get('camp', '')
        inf_id = params.get('inf', '')
        token = params.get('token', '')
        print 'Token:', token
        valid = True if encryptDecrypt(params.get('rand', ''), ip) == "valid" else False
        redirect_datetime = datetime.datetime.now()
        view_seconds = int(params.get('t', 0)) / 1000
        print "Valid click %s view_seconds in %s second" % (valid, view_seconds)
        if not valid:
            print "IP %s, cookie %s, token %s, offer id %s, validation -%s-" % (
                ip, cookie, token, offer_id, params.get('rand', ''))

        # Выделяем домен партнёра и добавляем его в целевой url
        print "Выделяем домен партнёра и добавляем его в целевой url"
        url = utmConverter(url, offer_id, campaign_id, inf_id, cookie)
        print "Create Task"
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
                                      view_seconds=view_seconds)
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
                                view_seconds=view_seconds)
            print ex
    except Exception as e:
        print e
        return _redirect_to(
            'https://yottos.com/?utm_source=yottos&utm_medium=redirect&utm_campaign=Not%20Valid%20Click', cookie)
    print 'Redirect complit to %s ' % (datetime.datetime.now() - elapsed_start_time).microseconds
    print "=" * 20
    return _redirect_to(
        url or 'https://yottos.com/?utm_source=yottos&utm_medium=redirect&utm_campaign=Not%20Valid%20Click', cookie)


def utmConverter(url, offer_id, campaign_id, inf_id, cookie):
    offer_info = _get_offer_info(offer_id, campaign_id)
    partner_domain = _get_informer(inf_id)
    offer_title = 'yottos-' + offer_info['title'].encode('utf-8').replace(' ', '_').replace('.', '_').replace(',',
                                                                                                              '_').replace(
        ';', '_').replace('!', '_').replace('?', '_')
    offer_campaign_title = 'yottos-' + offer_info['campaignTitle'].encode('utf-8').replace(' ', '_').replace('.',
                                                                                                             '_').replace(
        ',', '_').replace(';', '_').replace('!', '_').replace('?', '_')
    offer_title_trans = urllib.quote(_ful_trans(offer_title))
    offer_campaign_title_trans = urllib.quote(_ful_trans(offer_campaign_title))
    if offer_info['marker'][1]:
        offer_title = offer_title_trans
        offer_campaign_title = offer_campaign_title_trans
    else:
        offer_title = urllib.quote(offer_title)
        offer_campaign_title = urllib.quote(offer_campaign_title)
    url = _add_dynamic_param(url, partner_domain, offer_campaign_title, offer_title, offer_info['marker'][2])
    if offer_info['marker'][0]:
        url = _add_utm_param(url, type, partner_domain, offer_campaign_title, offer_title, offer_info['marker'][2],
                             cookie, offer_title_trans, offer_campaign_title_trans)
    print url
    return url


def encryptDecrypt(input, ip):
    key = list(ip)
    output = []

    for i in range(len(input)):
        xor_num = ord(input[i]) ^ ord(key[i % len(key)])
        output.append(chr(xor_num))

    return ''.join(output)


def _u8(string):
    return unicode(string, 'utf-8')


def _eu8(string):
    return string.encode('utf-8')


def _ful_trans(string):
    trans = _eu8(_u8(string.replace(' ', '-')).encode('trans').lower())
    return trans


def _add_url_param(url, param, value):
    ''' Добавляет параметр ``param`` со значением ``value`` в ``url`` если такого параметра не сушествует'''
    url_parts = list(urlparse.urlparse(url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    if not query.has_key(param):
        query.update({param: value})
    url_parts[4] = urllib.urlencode(query)
    return urlparse.urlunparse(url_parts)


def _url_param_safe_check(param):
    """
    Костыль для вот этого рекламодателя bonprix, потому что мы перед ним либизим :))
    """
    pattern = '(&{0,}[^&=]{0,}={1}[^&=]{0,})(\?)'
    if (re.search(pattern, param) is None):
        safe = True
    else:
        safe = False
    return safe


def _add_dynamic_param(url, source, campaign, name, hide):
    url_parts = list(urlparse.urlparse(url))

    params = dict(urlparse.parse_qsl(url_parts[3]))
    if ((len(params) > 0) and _url_param_safe_check(url_parts[3])):
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
    if ((len(query) > 0) and _url_param_safe_check(url_parts[4])):
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
        url_parts[4] = urllib.urlencode(query)
    print url_parts
    return urlparse.urlunparse(url_parts)


def _add_utm_param(url, type, source, campaign, name, hide, cookie, offer_title_trans, offer_campaign_title_trans):
    url_parts = list(urlparse.urlparse(url))
    if not _url_param_safe_check(url_parts[4]):
        return urlparse.urlunparse(url_parts)
    query = dict(urlparse.parse_qsl(url_parts[4]))

    if (type == 'banner'):
        utm_medium = 'cpm_yottos'
    else:
        utm_medium = 'cpc_yottos'
    if hide:
        utm_source = source['guid']
    else:
        utm_source = source['domain']

    utm_campaign = str(campaign)
    utm_content = str(name)

    if query.has_key('utm_source'):
        utm_term = utm_source
    else:
        utm_term = ''

    if not query.has_key('utm_medium'):
        query.update({'utm_medium': utm_medium})
    if not query.has_key('utm_source'):
        query.update({'utm_source': utm_source})
    if not query.has_key('utm_campaign'):
        query.update({'utm_campaign': utm_campaign})
    if not query.has_key('utm_content'):
        query.update({'utm_content': utm_content})
    if not query.has_key('utm_term'):
        query.update({'utm_term': utm_term})
    if not query.has_key('from'):
        query.update({'from': 'Yottos'})
    if not query.has_key('yt_u_id'):
        query.update({'yt_u_id': cookie})
    if not query.has_key('_openstat'):
        query.update({'_openstat': ';'.join([utm_medium, offer_campaign_title_trans, offer_title_trans, utm_source])})
    url_parts[4] = urllib.urlencode(query)
    return urlparse.urlunparse(url_parts)


def _get_informer(informer_id):
    ''' Возвращает домен, к которому относится информер ``informer_id`` '''
    try:
        db = pymongo.Connection(MONGO_HOST).getmyad_db
        inf = db.informer.find_one({'guid': informer_id})
        guid = inf.get('guid')
        guid_int = inf.get('guid_int')
        domain = inf.get('domain')
        domain = domain.replace('.', '_')
        return {
            'guid': str(guid),
            'guid_int': str(guid_int),
            'domain': str(domain.encode('utf8'))
        }
    except (AttributeError, KeyError):
        return {
            'guid': 'None',
            'guid_int': 'None',
            'domain': 'None'
        }
    except pymongo.errors.AutoReconnect:
        return {
            'guid': 'None',
            'guid_int': 'None',
            'domain': 'None'
        }


def _get_offer_info(offer_id, campaignId):
    ''' Возвращает True, если к ссылке перехода на рекламное предложение
        ``offer_id`` необходимо добавить маркер yottos_partner=... '''
    result = {'title': '', 'campaignTitle': '', 'marker': [True, False, False]}
    try:
        db = pymongo.Connection(MONGO_HOST).getmyad_db
        offer = db.offer.find_one({'guid': offer_id}, ['title'])
        campaign = db.campaign.find_one({'guid': campaignId}, ['title', 'yottosPartnerMarker', 'yottosTranslitMarker',
                                                               'yottosHideSiteMarker'])
        result['campaignTitle'] = campaign.get('title', 'NOT_TITLE')
        result['title'] = offer.get('title', 'NOT_TITLE')
        yottosPartnerMarker = campaign.get('yottosPartnerMarker', True)
        yottosTranslitMarker = campaign.get('yottosTranslitMarker', False)
        yottosHideSiteMarker = campaign.get('yottosHideSiteMarker', False)
        print "marker", [yottosPartnerMarker, yottosTranslitMarker, yottosHideSiteMarker]
        result['marker'] = [yottosPartnerMarker, yottosTranslitMarker, yottosHideSiteMarker]
        return result
    except (AttributeError, KeyError):
        return result
    except pymongo.errors.AutoReconnect:
        return result


application = redirect

if __name__ == '__main__':
    httpd = make_server('', 8000, application)
    httpd.serve_forever()
