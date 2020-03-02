#############################################################################
#Ingenieria de Datos -- oscartux@gmail.com
#Obtener metricas del API de Aircall
#Parametros Daily = Reporte del dia, This_Month= Reporte del mes
############################################################################

import requests
import codecs
import json
import datetime
import calendar
import sys
import time
import csv
from time import gmtime
import unicodedata



def WriteReponse(data,page):
    date = datetime.datetime.now().date()
    filename = str(date) + '.csv'
    fieldnames = ["call_status", "call_direction", "call_start_date", "call_answered_date",
                "call_end_date", "call_duration", "call_originating_number", "call_recording_link", "call_archived_status", "call_missed_reason",
                "call_cost", "call_number_name", "call_number_digits", "call_number_country", "call_answer_user_name", "call_answer_user_email", "call_answer_user_status",
                "call_tags", "call_comments"]
    longitud = len(data['calls'])
    index = 0
    while index < longitud:
        call_status = str(data['calls'][index]['status'])
        call_direction = str(data['calls'][index]['direction'])
        
        call_start_date = str('{}-{}-{} {}:{}:{}'.format(*
                                         gmtime(data['calls'][index]['started_at'])))
        call_answered_date = str('{}-{}-{} {}:{}:{}'.format(*
                                         gmtime(data['calls'][index]['answered_at'])))
        call_end_date = str('{}-{}-{} {}:{}:{}'.format(*
                                         gmtime(data['calls'][index]['ended_at'])))
        call_duration = str(data['calls'][index]['duration'])
        call_originating_number = str(data['calls'][index]['raw_digits'])
        call_recording_link = str(data['calls'][index]['recording'])
        call_archived_status = str(data['calls'][index]['archived'])
        call_missed_reason = str(data['calls'][index]['missed_call_reason'])
        call_cost = str(data['calls'][index]['cost'])
        call_number_name = str(data['calls'][index]['number']['name'])##NUmber
        call_number_digits = str(data['calls'][index]['number']['digits'])
        call_number_country = str(data['calls'][index]['number']['country'])


        if data['calls'][index]['user'] <> None:
            call_answer_user_name = (data['calls'][index]['user']['name']) ##User
            call_answer_user_name = unicodedata.normalize(
                'NFKD', call_answer_user_name).encode('ascii', 'ignore')
            call_answer_user_email = str(data['calls'][index]['user']['email'])
            call_answer_user_status = str(data['calls'][index]['user']['availability_status'])
        else:
            call_answer_user_name = ''
            call_answer_user_email = ''
            call_answer_user_status = ''
        
        ##tags
        if len(data['calls'][index]['tags']) > 0:
            index_tags = 0
            call_tags = ''
            while index_tags < len(data['calls'][index]['tags']):
                call_tags_pre = (data['calls'][index]
                                            ['tags'][index_tags]['name'])
                call_tags_carring = unicodedata.normalize(
                    'NFKD', call_tags_pre).encode('ascii', 'ignore')
                call_tags += call_tags_carring + '|'
                index_tags += 1
        else:
            call_tags = ''
        
        ##comments
        if len(data['calls'][index]['comments']) > 0:
            index_comments = 0
            call_comments = ''
            while index_comments < len(data['calls'][index]['comments']):
                call_comments_carring_pre = (data['calls'][index]
                                ['comments'][index_comments]['content'])
                call_comments_carring = unicodedata.normalize(
                    'NFKD', call_comments_carring_pre).encode('ascii', 'ignore')
                call_comments += call_comments_carring + '|'       
                index_comments += 1
        else:
            call_comments = ''
        
        ##Diccionario resultado
        call_detail_dicc = {'call_status': call_status, 
                           'call_direction': call_direction,
                           'call_start_date': call_start_date,
                           'call_answered_date': call_answered_date,
                           'call_end_date': call_end_date,
                           'call_duration': call_duration,
                           'call_originating_number': call_originating_number,
                           'call_recording_link': call_recording_link,
                           'call_archived_status': call_archived_status,
                           'call_missed_reason':call_missed_reason,
                           'call_cost': call_cost,
                           'call_number_name':call_number_name,
                           'call_number_digits':call_number_digits,
                           'call_number_country':call_number_country,
                           'call_answer_user_name':call_answer_user_name,
                           'call_answer_user_email':call_answer_user_email,
                           'call_answer_user_status':call_answer_user_status,
                           'call_tags': call_tags,
                           'call_comments':call_comments}
        if index == 0 and page == 1:
            with codecs.open(filename, 'w', encoding='utf-8') as file:##Nuevo file
                csvwriter = csv.DictWriter(file, fieldnames=fieldnames)
                csvwriter.writeheader()
                csvwriter.writerow(call_detail_dicc)
        elif index > 0 and page == 1:
            with codecs.open(filename, 'a+', encoding='utf-8') as file: ##Appends
                csvwriter = csv.DictWriter(file, fieldnames=fieldnames)
                csvwriter.writerow(call_detail_dicc)
        elif index == 0 and page > 1:
            with codecs.open(filename, 'a+', encoding='utf-8') as file: ##Appends
                csvwriter = csv.DictWriter(file, fieldnames=fieldnames)
                csvwriter.writerow(call_detail_dicc)
        elif index > 0 and page > 1:
            with codecs.open(filename, 'a+', encoding='utf-8') as file: ##Appends
                csvwriter = csv.DictWriter(file, fieldnames=fieldnames)
                csvwriter.writerow(call_detail_dicc)
 
        index += 1 ##Incrementa el contador para recorrer la data
    return call_detail_dicc
        

def main(argv):
    ##Params
    if len(sys.argv) > 1:
        report_type = sys.argv[1]
    else:
        report_type = 'Daily'

    ##Accesos API Aircall
    inbound_phone = '' ## Here comes the desired number to get the report from 
    usuario = ''## Here comes your username
    contrasena = ''## here comes your password
	



    ##Inicializamos valores
    page_index = 1
    more_pages = False


    ##Valida tipo reporte
    if report_type == 'Daily':
        today = datetime.datetime.now() - datetime.timedelta(days=1)
        today_beginning = datetime.datetime(
            today.year, today.month, today.day, 0, 0, 0, 0)
        today_beginning_time = int(time.mktime(today_beginning.timetuple()))
        today_end = datetime.datetime(
            today.year, today.month, today.day, 23, 59, 59, 999)
        today_end_time = int(time.mktime(today_end.timetuple()))
        ##Tiempo consulta
        beginning_time = today_beginning_time
        end_time = today_end_time
    elif report_type == 'This_Month':
        date = datetime.datetime.now().date()
        first_day_month = date.replace(day=1)
        last_day_month = date.replace(
            day=calendar.monthrange(date.year, date.month)[1])
        first_day_month_unix = int(time.mktime(first_day_month.timetuple()))
        last_day_month_unix = int(time.mktime(last_day_month.timetuple()))
        ##Tiempo consulta
        beginning_time = first_day_month_unix
        end_time = last_day_month_unix
    elif report_type == 'range':
        beginning_time = sys.argv[2]
        end_time = sys.argv[3]








    #Parametros de request
    url = "https://api.aircall.io/v1/calls/search/"
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    params = {'phone_number': inbound_phone, 'from': beginning_time,
            'to': end_time, 'direction': 'inbound', 'order': 'asc', 'page': page_index, 'per_page':'50'}

    #Request
    resp = requests.get(url, auth=(usuario, contrasena), headers=headers, params=params)
    resp.encoding = 'utf-8'

    ##Aqui tenemos la data lista
    data = json.loads(resp.text)
    WriteReponse(data,1)


    ##Validamos si hay paginado
    if data['meta']['next_page_link'] == None:
        more_pages = False
    else: 
        more_pages = True

    ##Mientras hay paginado hacemos el request
    while more_pages==True:
        page_index += 1
        params = {'phone_number': '+525547707341', 'from': beginning_time,
                'to': end_time, 'direction': 'inbound', 'order': 'asc', 'page': page_index, 'per_page': '50'} ##Comtrolamos la pagina
        resp = requests.get(url, auth=(usuario, contrasena),headers=headers, params=params)
        resp.encoding = 'utf-8'

        ##Aqui tenemos la data lista
        data = json.loads(resp.text)
        WriteReponse(data,page_index)
    
        ##Validamos de nuevo si hay paginado
        if data['meta']['next_page_link'] == None:
            more_pages = False
        else:
            more_pages = True


if __name__ == '__main__':
    main(sys.argv)

  






