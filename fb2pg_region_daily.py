#############################################################################
#Ingenieria de Datos -- oscartux@gmail.com
#Obtener reporte FB API
#Parametros fecha_inicia fecha_fin intervalo
############################################################################

##Lib
import sys
import json
import psycopg2
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi


since = sys.argv[1]
until = sys.argv[2]
time_increment = sys.argv[3]

##Iniciliza conexion DB
try:
    conn_string = "host='database server' port=5432 dbname='dbname' user='user_name' password='password'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
except Exception as error:
    print("No es posible conectarse a la BD se finaliza rutina",error)
    sys.exit(1)


##Datos de accesos FB
access_token='he comes the acces token from fb'
ad_account_id = 'the account id'
app_secret = 'the app secret'
app_id = 'the app id'
FacebookAdsApi.init(access_token=access_token)

##Fields para obtener los ads
fields_ads = [
    'name',
]
params_ads = {
    'effective_status': ['ACTIVE'],
}

###Fields para reporte 
fields = [
    'campaign_name',
    'adset_name',
    'ad_name',
    'reach',
    'impressions',
    'frequency',
    'cpm',
    'cpp',
    'cpc',
    'ctr',
    'spend',
    'video_p25_watched_actions',
    'video_p50_watched_actions',
    'video_p75_watched_actions',
    'video_p100_watched_actions',
    'unique_outbound_clicks',
    'actions',
    'cost_per_action_type',]
params = {
    'time_range':{'since':since,'until':until},
    'time_increment': time_increment,
    'level': 'ad',
    'breakdowns': ['region'],
    'filtering': [{'field': 'action_type', 'operator': 'IN', 'value': ['omni_app_install', 'video_view', 'outbound_click', 'landing_page_view', 'offsite_conversion.custom.2138442442894981', 'offsite_conversion.custom.508120819727119']}],

}

##Llamada al API
obj_ads = (AdAccount(ad_account_id).get_ads(
    fields=fields_ads,
    params=params_ads,
))
##Consulta a la queue de resultados
obj_ads_list = obj_ads._queue

##Recorre el diccionario
for dicc in obj_ads_list:
    nombre_ad=dicc._json['name']
    ad_id=dicc._json['id']
    obj_stats_ad = estadisticas_ad = (Ad(ad_id).get_insights(  ## Llamada al API para obtener los stats
    fields=fields,
    params=params,
    ))

    obj_stats_ad_list = obj_stats_ad._queue ## Consulta a la queue de resultados

    ##Recorre dicc
    for dicc_ad in obj_stats_ad_list:
        json_reponse = dicc_ad._json        ##JSON con resultados

        #######Names

        campaign_name_value = json_reponse['campaign_name']
        adset_name_value = json_reponse['adset_name']
        ad_name_value = json_reponse['ad_name']

        ####### breakdowns result values
        if 'region' in json_reponse:
            region_value = json_reponse['region']
        else:
            region_value = ''


        ####### fields result values
        if 'cpc' in json_reponse:
            cpc_value = json_reponse['cpc']  
        else:
            cpc_value= '0'

        if 'cpm' in json_reponse:
            cpm_value = json_reponse['cpm']
        else:
            cpm_value = '0'

        if 'cpp' in json_reponse:
            cpp_value = json_reponse['cpp']
        else: 
            cpp_value = '0'

        if 'ctr' in json_reponse:
            ctr_value = json_reponse['ctr']
        else: 
            ctr_value = '0'

        if 'date_start' in json_reponse:
            report_date_start_value = json_reponse['date_start']
        else:
            report_date_start_value = ''

        if 'date_stop' in json_reponse:
            report_date_stop_value = json_reponse['date_stop']
        else:
            report_date_stop_value = ''

        if 'frequency' in json_reponse:
            frequency_value = json_reponse['frequency']
        else:
            frequency_value = '0'

        if 'impressions' in json_reponse:
            impressions_value = json_reponse["impressions"]
        else:
            impressions_value = '0'

        if 'reach' in json_reponse:
            reach_value = json_reponse['reach']
        else:
            reach_value ='0'

        if 'spend' in json_reponse:
            spend_value = json_reponse['spend']
        else:
            spend_value = '0'

        if 'video_p25_watched_actions' in json_reponse:
            video_p25_watched_actions_value = json_reponse['video_p25_watched_actions'][0]['value']
        else:
            video_p25_watched_actions_value = '0'
        
        if 'video_p50_watched_actions' in json_reponse:
            video_p50_watched_actions_value = json_reponse['video_p50_watched_actions'][0]['value']
        else:
            video_p50_watched_actions_value ='0'

        if 'video_p75_watched_actions' in json_reponse:
            video_p75_watched_actions_value = json_reponse['video_p75_watched_actions'][0]['value']
        else:
            video_p75_watched_actions_value = '0'


        if 'video_p100_watched_actions' in json_reponse:
            video_p100_watched_actions_value = json_reponse['video_p100_watched_actions'][0]['value']
        else:
            video_p100_watched_actions_value = '0'

        if 'unique_outbound_clicks' in json_reponse:
            unique_outbound_clicks_value = json_reponse['unique_outbound_clicks'][0]['value']
        else:
            unique_outbound_clicks_value = '0'

        ##Logica para  accion
        if 'actions' in json_reponse:

            actions_count = 0
            longitud_actions = len(json_reponse['actions'])

            while actions_count < longitud_actions:

                if json_reponse['actions'][actions_count]['action_type'] == 'landing_page_view':
                    landing_page_view_value = json_reponse['actions'][actions_count]['value']
                else:
                    landing_page_view_value = '0'

                if json_reponse['actions'][actions_count]['action_type'] == 'omni_app_install':
                    omni_app_install_value = json_reponse['actions'][actions_count]['value']
                else:
                    omni_app_install_value = '0'

                if json_reponse['actions'][actions_count]['action_type'] == 'offsite_conversion.custom.2138442442894981':
                    registro_checkout_actualizada = json_reponse['actions'][actions_count]['value']
                else:
                    registro_checkout_actualizada = '0'

                if json_reponse['actions'][actions_count]['action_type'] == 'offsite_conversion.custom.508120819727119':
                    boton_visita_revamp = json_reponse['actions'][actions_count]['value']
                else:
                    boton_visita_revamp = '0'
                
                actions_count += 1
        else:
            landing_page_view_value = '0'
            omni_app_install_value = '0'
            registro_checkout_actualizada = '0'
            boton_visita_revamp = '0'

      ##Logica para costo por accion  
        if 'cost_per_action_type' in json_reponse:
            actions_cost_count = 0
            longitud_actions_cost = len(json_reponse['cost_per_action_type'])

            while actions_cost_count < longitud_actions_cost:

                if json_reponse['cost_per_action_type'][actions_cost_count]['action_type'] == 'landing_page_view':
                    landing_page_view_cost = json_reponse['cost_per_action_type'][actions_cost_count]['value']
                else:
                    landing_page_view_cost = '0'

                if json_reponse['cost_per_action_type'][actions_cost_count]['action_type'] == 'omni_app_install':
                    omni_app_install_cost = json_reponse['cost_per_action_type'][actions_cost_count]['value']
                else:
                    omni_app_install_cost = '0'

                if json_reponse['cost_per_action_type'][actions_cost_count]['action_type'] == 'offsite_conversion.custom.2138442442894981':
                    registro_checkout_actualizada_cost = json_reponse['cost_per_action_type'][actions_cost_count]['value']
                else:
                    registro_checkout_actualizada_cost = '0'

                if json_reponse['cost_per_action_type'][actions_cost_count]['action_type'] == 'offsite_conversion.custom.508120819727119':
                    boton_visita_revamp_cost = json_reponse['cost_per_action_type'][actions_cost_count]['value']
                else:
                    boton_visita_revamp_cost = '0'

                actions_cost_count += 1
        else:
            landing_page_view_cost = '0'
            omni_app_install_cost = '0'
            registro_checkout_actualizada_cost = '0'
            boton_visita_revamp_cost = '0'

        ##Here comes the db
        try:
            cursor.execute("""INSERT INTO data_engineering.tbl_fb_ad_daily_region
            (report_start_date, report_end_date, campaign_name, adset_name, ad_name, region, cpc, cpm, cpp, ctr, frequency, impressions, reach, spend, video_p25_watched_actions, video_p50_watched_actions, video_p75_watched_actions, video_p100_watched_actions, unique_outbound_clicks, app_installs, app_installs_cost, landing_page_views,registro_checkout_actualizada,registro_checkout_actualizada_cost,boton_visita_revamp,boton_visita_revamp_cost)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s)""", 
            [report_date_start_value,report_date_stop_value,campaign_name_value,adset_name_value,ad_name_value,region_value,cpc_value,cpm_value,cpp_value,ctr_value,frequency_value,impressions_value,reach_value,spend_value,video_p25_watched_actions_value,video_p50_watched_actions_value,video_p75_watched_actions_value,video_p100_watched_actions_value,unique_outbound_clicks_value,omni_app_install_value,omni_app_install_cost,landing_page_view_value,registro_checkout_actualizada,registro_checkout_actualizada_cost,boton_visita_revamp,boton_visita_revamp_cost])
            result_trans = 0
        except Exception as error:
            print("No fue posible insertar el registro: ", error)
            result_trans = 1
    ##Commit hasta el final para reducir impacto
    if result_trans == 0:
        conn.commit()
cursor.close()
conn.close()





        


        









 





