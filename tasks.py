# encoding: utf-8
# This Python file uses the following encoding: utf-8
from celery.task import task, periodic_task
import ConfigParser
import datetime
import uuid
import dateutil.parser
import os
import socket
import pymongo
import re
import pymssql

MONGO_HOST = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
MONGO_WORKER_HOST_POOL = ['srv-3.yottos.com:27017','srv-6.yottos.com:27017','srv-7.yottos.com:27017',
                          'srv-8.yottos.com:27017','srv-9.yottos.com:27017',
                          'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020']
MONGO_DATABASE = 'getmyad_db'
MONGO_WORKER_DATABASE = 'getmyad'
otype = type

def _mongo_connection(host):
    u"""Возвращает Connection к серверу MongoDB"""
    try:
        connection = pymongo.Connection(host=host)
    except pymongo.errors.AutoReconnect:
        # Пауза и повторная попытка подключиться
        from time import sleep
        sleep(1)
        connection = pymongo.Connection(host=host)
    return connection

def _mongo_main_db():
    u"""Возвращает подключение к базе данных MongoDB"""
    return _mongo_connection(MONGO_HOST)[MONGO_DATABASE]

def _mongo_worker_db_pool():
    u"""Возвращает подключение к базе данных MongoDB"""
    pool = []
    for host in MONGO_WORKER_HOST_POOL:
        try:
            pool.append(_mongo_connection(host)[MONGO_WORKER_DATABASE])
        except Exception as e:
            print e, host
    return pool

def mssql_connection_adload():
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-1.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='1gb_YottosAdLoad',
                           as_dict=True,
                           charset='cp1251')
    conn.autocommit(True)
    return conn

def currencyCost(currency):
    ''' Возвращает курс валюты ``currency``.

        Если валюта не найдена, вернёт 0.
    '''
    connection_adload = mssql_connection_adload()
    cursor = connection_adload.cursor()
    cursor.execute('select cost from GetMyAd_CurrencyCost where currency=%s', (currency,))
    row = cursor.fetchone()
    if not row:
        cursor.close()
        #app_globals.connection_adload.close()
        return 0.0
    cursor.close()
    #app_globals.connection_adload.close()
    return float(row['cost'])

def addClick(offer_id, campaign_id, click_datetime=None, social=None):
    ''' Запись перехода на рекламное предложение ``offer_id`` с адреса
        ``ip`.

        ``offer_id``
            GUID рекламного предложения.

        ``ip``
            IP посетителя, сделавшего клик.
        
        ``click_datetime``
            Дата и время, за которую будет записан клик. По умолчанию
            принимается за текущее время. Если передаётся, то должно
            быть сторокой в ISO формате (YYYY-MM-DDTHH:MM:SS[.mmmmmm]).

        Возвращает структуру следующего формата::
            
            {'ok': Boolean,     # Успешно ли выполнилась операция
             'error': String,   # Описание ошибки, если ok == False
             'cost': Decimal    # Сумма, списанная с рекламодателя (в $)
            }
    '''
    try:
        uuid.UUID(offer_id)
    except ValueError, ex:
        print ex
        return {'ok': False, 'error': 'offer_id should be uuid string! ' + ex}

    try:
        dt = dateutil.parser.parse(click_datetime)
    except (ValueError, AttributeError):
        dt = datetime.datetime.now()

    if social is None:
        social = False
    
    try:        
        connection_adload = mssql_connection_adload()
        cursor = connection_adload.cursor()
        click_cost = 0.0 
        
        # Записываем переход
        print "Записываем переход"
        social = int(social)
        try:
            cursor.execute('''exec ClickAdd @LotID=%s, @AdvertiseID=%s, @DateView=%s, @Social=%s ''', (offer_id, campaign_id, dt, social))
            cursor.nextset()
            row = cursor.fetchone()
            click_cost = float(row['ClickCost'])
        except Exception as ex:
            print ex
            return {'ok': False, 'error': str(ex)}

        # Пересчёт стоимости клика по курсу
        print "Пересчёт стоимости клика по курсу"
        if not social:
            currency_cost = currencyCost('$')
            if currency_cost > 0:
                click_cost /= currency_cost
            cursor.close()
        else:
            click_cost = 0.0
        if not social and click_cost == 0.0:
            return {'ok': False, 'error': "adload click cost 0"}
        print  "Offer: ", offer_id, "Cost - ", click_cost
        return {'ok': True, 'cost': click_cost}
    
    except Exception, ex:
        print ex
        cursor.close()
        return {'ok': False, 'error': str(ex)}

@task
def process_click(url,
                  ip,
                  click_datetime,
                  offer_id,
                  campaign_id,
                  informer_id,
                  token,
                  valid,
                  referer,
                  user_agent,
                  cookie,
                  view_seconds):
    ''' Обработка клика пользователя по рекламному предложению.

        Задача ставится в очередь скриптом redirect.py или выполняется
        немедленно при недоступности Celery.

        В процессе обработки:

        1. IP ищется в чёрном списке.

        2. Проверяется, что по ссылке переходит тот же ip, которому она была
           выдана.

        3. Проверяется, что ссылка ещё не устарела.

        4. Если ip сделал больше трёх переходов за сутки, ip заносится в чёрный
           список.

        5. Клик передаётся в AdLoad.

        6. Только если все предыдущие пункты отработали нормально, клик
           записывается в GetMyAd. В противном случае, делается запись либо
           в ``clicks.rejected`` (отклонённые клики), либо в ``clicks.error``
           (клики, во время обработки которых произошла ошибка).

        ERROR ID LIST
        1 - Несовпадает Токен и IP
        2 - Найден в Чёрном списке IP
        3 - Более 3 переходов с РБ за сутки
        4 - Более 10 переходов с РБ за неделю
        5 - Более 5 переходов с ПС за сутки
        6 - Более 10 переходов с ПС за неделю
    '''
    print "/----------------------------------------------------------------------/"
    print "process click %s \t %s" % (ip, click_datetime)
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    
    def log_error(reason):
        db.clicks.error.insert({'ip': ip, 'offer': offer_id, 'dt': click_datetime,
                                'title': title, 'token': token,
                                'inf': informer_id, 'url': url, 'reason': reason,
                                'errorId': errorId, 'city': city, 'country': country,
                                'campaignId': campaign_id, 'referer':referer, 'user_agent':user_agent, 'cookie':cookie,
                                'view_seconds':view_seconds, 'campaignTitle': campaign_title},
                                safe=True)
    def log_reject(reason):
        db.clicks.rejected.insert({'ip': ip, 'offer': offer_id, 'dt': click_datetime,
                                   'title': title,  'token': token,
                                   'inf': informer_id, 'url': url, 'reason': reason,
                                   'errorId': errorId, 'city': city, 'country': country,
                                   'campaignId': campaign_id, 'referer':referer, 'user_agent':user_agent, 'cookie':cookie,
                                   'view_seconds':view_seconds, 'campaignTitle': campaign_title},
                                   safe=True)

    def _get_user_id(informer_id):
        user = db.informer.find_one({'guid': informer_id}, ['user',])
        guid = db.users.find_one({'login':user['user']}, {'guid':1, '_id':0}).get("guid","")
        print "GetmyadID %s" % guid
        return guid

    def _get_manager(informer_id):
        user = db.informer.find_one({'guid': informer_id}, ['user',])
        manager_g = db.users.find_one({'login':user['user']}, {'managerGet':1, '_id':0}).get("managerGet","")
        print "Getmyad Manager %s" % manager_g
        return manager_g

    def _partner_click_cost(informer_id, adload_cost):
        ''' Возвращает цену клика для сайта-партнёра.
            
            ``informer_id``
                ID информера, по которому произошёл клик.

            ``adload_cost``
                Цена клика для рекламодателя. Используется в случае плавающей
                цены.
        '''
        try:
            user = db.informer.find_one({'guid': informer_id}, ['user','cost'])
            if user.get('cost','None') == 'None':
                userCost = db.users.find_one({'login':user['user']}, {'cost':1, '_id':0})
                percent = int(userCost.get('cost',{}).get('ALL',{}).get('click',{}).get('percent',50))
                cost_min = float(userCost.get('cost',{}).get('ALL',{}).get('click',{}).get('cost_min',  0.01))
                cost_max = float(userCost.get('cost',{}).get('ALL',{}).get('click',{}).get('cost_max', 1.00))
                print "Account COST percent %s, cost_min %s, cost_max %s"% (percent, cost_min, cost_max)
            else:
                percent = int(user.get('cost',{}).get('ALL',{}).get('click',{}).get('percent',50))
                cost_min = float(user.get('cost',{}).get('ALL',{}).get('click',{}).get('cost_min',  0.01))
                cost_max = float(user.get('cost',{}).get('ALL',{}).get('click',{}).get('cost_max', 1.00))
                print "Informer COST percent %s, cost_min %s, cost_max %s"% (percent, cost_min, cost_max)

            cost = round(adload_cost * percent / 100, 2)
            if cost_min and cost < cost_min:
                cost = cost_min
            if cost_max and cost > cost_max:
                cost = cost_max
        except Exception as e :
            print e
            cost = 0
        return cost

    def _partner_blocked(informer_id):
        user_name = db.informer.find_one({'guid': informer_id}, ['user'])
        user = db.users.find_one({'login':user_name['user']})
        result = {
                'block': False,
                'filter': False,
                'time_filter_click': 15
                }
        block = user.get('blocked', False)
        result['time_filter_click'] = user.get('time_filter_click', 15)
        if block:
            if block == 'banned':
                result['block'] = True
            elif block == 'light':
                result['block'] = True
            elif block == 'filter':
                result['block'] = False
                result['filter'] = True
            else:
                result['block'] = False
        print "Check Account", result
        return result
        
    # С тестовыми кликами ничего не делаем
    title = 'Non Title'
    account_id = None
    campaignTitle = 'Non Title'
    social = False
    country = 'NOT FOUND'
    city = 'NOT FOUND'
    branch = 'L0'
    conformity = ''
    matching = ''
    test = False
        
    for db2 in pool:
        find = False
        try:
            for x in db2.log.impressions.find({'token': token}):
                print db2
                print "ip %s = %s" % (x['ip'],ip)
                print "id %s = %s" % (x['id'], offer_id)
                if x['ip'] == ip and x['id'] == offer_id:
                    valid = True
                    title = x.get('title', 'Non Title')
                    account_id = x.get('account_id', None)
                    campaignTitle = x.get('campaignTitle', 'Non Title')
                    social = x.get('social', False)
                    country = x.get('country', 'NOT FOUND')
                    city = x.get('region', 'NOT FOUND')
                    branch = x.get('branch', '')
                    conformity = x.get('conformity', '')
                    matching = x.get('matching', '')
                    test = x.get('test', False)
                    find = True
                    break

            if find:
                break
        except Exception as e:
            print e
            pass
    
    if test:
        print "Processed test click from ip %s" % ip
        return

    # Определяем кампанию, к которой относится предложение
    try:
        of = db.offer.find_one({'guid': offer_id}, {'campaignId': True, 'campaignTitle': True, 'category':True, '_id': False})
        campaign_id = of.get('campaignId',None)
        campaign_title = of.get('campaignTitle',None)
        category = of.get('category',None)
        manager = db.campaign.find_one({'guid': campaign_id}, {'manager':True, '_id': False}).get('manager','').encode('utf-8')

    except Exception as e:
        print e
        campaign_id = None
        campaign_title = None
        category = None
    
    errorId = 0
    manager_g = _get_manager(informer_id)
    try:
        print "Token = %s" % token.encode('utf-8')
        print "Cookie = %s" % cookie
        print "IP = %s" % ip
        print "REFERER = %s" % referer.encode('utf-8')
        print "USER AGENT = %s" % user_agent.encode('utf-8')
        print "OfferId = %s" % offer_id.encode('utf-8')
        print "Offer Title = %s" % title.encode('utf-8')
        print "Informer = %s" % informer_id.encode('utf-8')
        print "Campaign Title = %s " % campaign_title.encode('utf-8')
        print "CampaignId = %s" % campaign_id.encode('utf-8')
        print "Branch = %s" % branch
        print "User View-Click = %s" % view_seconds
        print "Manager = %s" % manager    
    except Exception as e:
        print e
    if not valid:
        errorId = 1
        log_reject(u'Не совпадает токен или ip')
        print "token ip false click rejected"
        return
    print "Geo = country %s, city %s " % (country.encode('utf-8'), city.encode('utf-8'))
    # Ищём IP в чёрном списке
    if db.blacklist.ip.find_one({'ip': ip, 'cookie': cookie}):
        errorId = 2
        print "Blacklisted ip:", ip, cookie
        log_reject("Blacklisted ip")
        return
    
    # Ищем, не было ли кликов по этому товару
    # Заодно проверяем ограничение на MAX_CLICKS_FOR_ONE_DAY переходов в сутки
    # (защита от накруток)
    MAX_CLICKS_FOR_ONE_DAY = 3
    MAX_CLICKS_FOR_ONE_DAY_ALL = 5
    MAX_CLICKS_FOR_ONE_WEEK = 10
    MAX_CLICKS_FOR_ONE_WEEK_ALL = 10
    unique = True
    #Проверяе по рекламному блоку за день и неделю
    today_clicks = 0
    toweek_clicks = 0
    for click in db.clicks.find({'ip': ip, 'cookie': cookie, 'inf': informer_id, 'dt': {'$lte': click_datetime, '$gte': (click_datetime - datetime.timedelta(weeks=1))}}).limit(MAX_CLICKS_FOR_ONE_DAY + MAX_CLICKS_FOR_ONE_WEEK):
        if (click_datetime - click['dt']).days == 0:
            today_clicks += 1
            toweek_clicks += 1
        else:
            toweek_clicks +=1

        if click['offer'] == offer_id:
            unique = False

    print "Total clicks for day in informers = %s" % today_clicks
    if today_clicks >= MAX_CLICKS_FOR_ONE_DAY:
        errorId = 3
        log_reject(u'Более %d переходов с РБ за сутки' % MAX_CLICKS_FOR_ONE_DAY)
        unique = False
        print 'Many Clicks for day to informer'
        db.blacklist.ip.update({'ip': ip, 'cookie': cookie},
                               {'$set': {'dt': datetime.datetime.now()}},
                               upsert=True)

    print "Total clicks for week in informers = %s" % toweek_clicks
    if toweek_clicks >= MAX_CLICKS_FOR_ONE_WEEK:
        errorId = 4
        log_reject(u'Более %d переходов с РБ за неделю' % MAX_CLICKS_FOR_ONE_WEEK)
        unique = False
        print 'Many Clicks for week to informer'
        db.blacklist.ip.update({'ip': ip, 'cookie':cookie},
                               {'$set': {'dt': datetime.datetime.now()}},
                               upsert=True)
    #Проверяе по ПС за день и неделю
    today_clicks_all = 0
    toweek_clicks_all = 0
    for click in db.clicks.find({'ip': ip, 'cookie': cookie, 'dt': {'$lte': click_datetime, '$gte': (click_datetime - datetime.timedelta(weeks=1))}}).limit(MAX_CLICKS_FOR_ONE_WEEK_ALL + MAX_CLICKS_FOR_ONE_DAY_ALL):
        if (click_datetime - click['dt']).days == 0:
            today_clicks_all += 1
            toweek_clicks_all += 1
        else:
            toweek_clicks_all +=1

        if click['offer'] == offer_id:
            unique = False

    print "Total clicks for day in all partners = %s" % today_clicks_all
    if today_clicks_all >= MAX_CLICKS_FOR_ONE_DAY_ALL:
        errorId = 5
        log_reject(u'Более %d переходов с ПС за сутки' % MAX_CLICKS_FOR_ONE_DAY_ALL)
        unique = False
        print 'Many Clicks for day to all partners'
        db.blacklist.ip.update({'ip': ip, 'cookie': cookie},
                               {'$set': {'dt': datetime.datetime.now()}},
                               upsert=True)

    print "Total clicks for week in all partners = %s" % toweek_clicks_all
    if toweek_clicks_all >= MAX_CLICKS_FOR_ONE_WEEK_ALL:
        errorId = 6
        log_reject(u'Более %d переходов с ПС за неделю' % MAX_CLICKS_FOR_ONE_WEEK_ALL)
        unique = False
        print 'Many Clicks for week to all partners'
        db.blacklist.ip.update({'ip': ip, 'cookie': cookie},
                               {'$set': {'dt': datetime.datetime.now()}},
                               upsert=True)
    check_block = _partner_blocked(informer_id)
    if (check_block['block']):
        print "Account block"
        errorId = 7
        log_reject(u'Account block')
        return
    if (check_block['filter']):
        print "Account filtered"
        print check_block['time_filter_click']
        if int(view_seconds) < int(check_block['time_filter_click']):
            errorId = 8
            log_reject(u"Click %s View Seconds %s" % (int(view_seconds), int(check_block['time_filter_click'])))
            print "Click %s View Seconds %s" % (int(view_seconds), int(check_block['time_filter_click']))
            return
    getmyad_user_id = _get_user_id(informer_id)    
    adload_cost = 0
    cost = 0
    # Сохраняем клик в AdLoad
    adload_ok = True
    try:
        if unique:
            print "Adload request"
            adload_response = addClick(offer_id, campaign_id, click_datetime.isoformat(), social)
            adload_ok = adload_response.get('ok', False)
            print "Adload OK - %s" % adload_ok
            if not adload_ok and 'error' in adload_response:
                errorId = 0
                log_error('Adload вернул ошибку: %s' %
                          adload_response['error'])
            adload_cost = adload_response.get('cost', 0)
            print "Adload COST %s" % adload_cost
    except Exception, ex:
        adload_ok = False
        errorId = 0
        log_error(u'Ошибка при обращении к adload: %s' % str(ex))
        print "adload failed"
    # Сохраняем клик в GetMyAd
    click_obj = {"ip": ip,
                      "city": city,
                      "country": country,
                      "offer": offer_id,
                      "campaignId": campaign_id,
                      "campaignTitle": campaign_title,
                      "title": title,
                      "dt": click_datetime,
                      "inf": informer_id,
                      "account_id": account_id,
                      "getmyad_user_id": getmyad_user_id,
                      "unique": unique,
                      "cost": cost,
                      "adload_cost": adload_cost,
                      "income": adload_cost - cost,
                      "url": url,
                      "branch": branch,
                      "conformity": conformity,
                      "matching": matching,
                      "social":social,
                      "referer":referer,
                      "user_agent":user_agent,
                      "cookie":cookie,
                      "adload_manager": manager,
                      "getmyad_manager": manager_g,
                      "view_seconds":view_seconds}
    if not social and adload_ok:
        cost = _partner_click_cost(informer_id, adload_cost) if unique else 0
        print "Payable click at the price of %s" % cost
        click_obj['cost'] = cost
        click_obj['income'] = adload_cost - cost
    elif social and adload_ok:
        print "Social click"
    else:
        print "No click"
        return
    db.clicks.insert(click_obj, safe=True)

    print "Click complite"
    print "/----------------------------------------------------------------------/"
