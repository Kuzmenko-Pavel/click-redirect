# -*- coding: UTF-8 -*-
'''
Created on Jul 20, 2010

@author: silver

'''
import json
from amqplib import client_0_8 as amqp


class MQ(object):
    '''
    Класс отвечает за отправку сообщений в RabbitMQ.
    '''

    def __init__(self):
        pass

    def _get_channel(self):
        ''' Подключается к брокеру mq '''
        conn = amqp.Connection(host='amqp.yottos.com',
                               userid='worker',
                               password='worker',
                               virtual_host='proxy_google_analytics',
                               insist=True)
        ch = conn.channel()
        ch.exchange_declare(exchange="proxy_google_analytics", type="topic", durable=True, auto_delete=False,
                            passive=False)
        return ch

    def click(self, url, ip, click_datetime, offer_id, campaign_id, informer_id, token, referer, user_agent, account_id,
              adload_cost, cid):
        ''' Отправляет уведомление о запуске рекламной кампании ``campaign_id`` '''
        ch = self._get_channel()
        data = {
            'url': url,
            'ip': ip,
            'click_datetime': click_datetime,
            'offer_id': offer_id,
            'campaign_id': campaign_id,
            'informer_id': informer_id,
            'token': token,
            'referer': referer,
            'user_agent': user_agent,
            'account_id': account_id,
            'adload_cost': adload_cost,
            'cid': cid
        }
        msg = amqp.Message(json.dumps(data))
        ch.basic_publish(msg, exchange='proxy_google_analytics', routing_key='action.click')
        ch.close()
        print "AMQP event click"
