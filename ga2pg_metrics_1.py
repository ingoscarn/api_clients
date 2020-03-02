#############################################################################
#Ingenieria de Datos -- oscartux@gmail.com
#Obtener metricas del API de Google Analytics v4.0
#Parametros fecha_ini y fecha_fin
############################################################################


import psycopg2  
import sys
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials



##Rango de fechas del reporte
start_date = sys.argv[1]
end_date = sys.argv[2]



table='data_engineering.dte_tbl_ga_metrics_1'



##Configuracion de acceso
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'path/to/json.json'
VIEW_ID = 'the view id'
##Inicializa el auth
def initialize_analyticsreporting():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

#Genera la peticion al API con la metrica
def get_report(analytics):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': start_date ,'endDate': end_date}],
          'metrics': [{'expression': 'ga:users'},{'expression': 'ga:newUsers'},{'expression': 'ga:sessions'},{'expression': 'ga:bounces'},{'expression': 'ga:pageviewsPerSession'},{'expression': 'ga:avgSessionDuration'},{'expression': 'ga:goal13Completions'},{'expression': 'ga:goal13ConversionRate'},{'expression': 'ga:goal3Completions'},{'expression': 'ga:goal3ConversionRate'}],
          'dimensions': [{'name': 'ga:source' },{'name': 'ga:medium'},{'name': 'ga:campaign'},{'name': 'ga:userAgeBracket'},{'name': 'ga:userGender'},{'name': 'ga:deviceCategory'},{'name': 'ga:date'}]
        }]
      }
  ).execute()


def main():
  
	##Iniciliza conexion
	conn_string = "host='athena-dev-rw.kavak.services' port=5432 dbname='kavak' user='oscar_nunez' password='N6n3p0ROxYA0bCN$YQcjhVB*zuNGnEHr'"
	conn = psycopg2.connect(conn_string)

	##Se crea un cursor
	cursor = conn.cursor()
	#Iniciamos el api
	analytics = initialize_analyticsreporting()
	#Guardamos el response en JSON
	response = get_report(analytics)

	for report in response.get('reports', []): ##Iteracion sobre reports
		for row in report.get('data', {}).get('rows', []):
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])
			for value in dateRangeValues:
					cursor.execute("""INSERT INTO """ + table +"""  (source, medium, campaign, userAgeBracket, userGender, deviceCategory, date, users, new_users, sessions, bounces, pageviews_per_session, avg_session_duration,goal_13_completions,goal_13_conversion_rate,goal_3_completions,goal_3_conversion_rate)
					        VALUES(%s, %s, %s, %s ,%s ,%s, %s, %s,%s, %s, %s, %s ,%s ,%s, %s, %s, %s )""", [dimensions[0], dimensions[1], dimensions[2], dimensions[3], dimensions[4], dimensions[5] ,dimensions[6] , value['values'][0], value['values'][1], value['values'][2], value['values'][3], value['values'][4], value['values'][5], value['values'][6], value['values'][7], value['values'][8] , value['values'][9] ])
					
	##Commit hasta el final para reducir impacto				
	conn.commit()
	cursor.close()
	conn.close()

if __name__ == '__main__':
  main()
