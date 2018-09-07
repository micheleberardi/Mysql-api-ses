# -*- coding: UTF-8 -*-
# enable debugging
import urllib2
import json
import urllib2
import simplejson
import MySQLdb
import logging
import boto3
import sys
import smtplib
import hashlib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from botocore.exceptions import ClientError
from datetime import datetime
from dateutil.relativedelta import relativedelta

#CHECK DATE
date_after_day = datetime.today()+ relativedelta(days=-1)
date  = date_after_day.strftime('%Y-%m-%d')

#LOGGING SECTION
logging.basicConfig(filename='logs/reports.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

#CONNECTION DATABASE TO SAVE TOKEN

mydb = MySQLdb.connect(host='10.x.x.x', user='user', passwd='pwd', db='db_name', use_unicode=True, charset="utf8")
cursor = mydb.cursor()

#GET THE LIST USERNAME
select_id = "SELECT USERNAME FROM report"
cursor.execute(select_id)
list = cursor.fetchall()
id = list

for query in id:
	try:
		#print query
		#SELECT TO GET THE USERNAME AND PASSWORD
		select_credentials = 'SELECT USERNAME, PASSWORD, EMAIL, EXCHANGE FROM report where username = "{0}"'.format(query[0])
		#print select_credentials
		cursor.execute(select_credentials)
        	credentials = cursor.fetchall()[0]
        	user = credentials[0]
        	passwd = credentials[1]
        	email = credentials[2]
        	exchange = credentials[3]
		m = hashlib.md5()
		m.update(passwd)
	except Exception as e:
         logger.error(e)
         pass

	try:
		#CREATE TOKEN
		response = urllib2.urlopen("https://api.domain.com/3.0/login/?username="+user+"&password="+m.hexdigest()+"")
		result = simplejson.load(response)
		token1 =  result["data"][0]["token"]
		#SAVE THE TOKEN INTO DB
		update_statement = 'UPDATE report SET TOKEN="{0}" WHERE USERNAME="{1}"'.format(token1, user)
		cursor.execute(update_statement)
		mydb.commit()
        except Exception as e:
         logger.error(e)
         pass

        try:

		#CALL API AND GET THE INFO
		response = urllib2.urlopen("https://api.domain.com/3.0/report/thirdpart/?token="+token1+"&type=1&method=getdata&metrics=bid_response,wins,bucksense_cost&start_date="+date+"T00:00:00.000&end_date="+date+"T23:59:59.000&timezone=America/New_York&groupby=P1D")
		result = simplejson.load(response)
		wins =  result["data"]["result"]["data"][0]["event"]["wins"]
		bid_response =  result["data"]["result"]["data"][0]["event"]["bid_response"]
		bucksense_cost = "0"
		#UPDATE DATABASE
		update_result = 'UPDATE report SET bid_response="{0}", wins = "{1}" WHERE USERNAME="{2}"'.format(bid_response, wins, user)
		cursor.execute(update_result)
		mydb.commit()
        except Exception as e:
         logger.error(e)
         pass

        try:

		#SEND REPORT THRU AMAZON SES
		name = exchange
		from_address = "pippo@domain.com"
		to_address = email
		bcc_address = "michymak@domain.com"
		subject = "Subject Reports :{}".format(date)
		msg = MIMEMultipart()
		msg['From'] = from_address
		msg['To'] = to_address
		msg['Bcc'] = bcc_address
		msg['Subject'] = "Reports: {}".format(subject)
		body = """
		Hello, {0}.

		Report {3}

		wins = {1}
		bid_response = {2}
		bucksense_cost = 
		

		Kind Regards,

		Bucksense Team

		""".format(name, wins, bid_response, date)

		msg.attach(MIMEText(body, 'plain'))
		server = smtplib.SMTP('email-smtp.us-east-1.amazonaws.com', 587)
		server.starttls()
		server.login("user", "pwd")
		text = msg.as_string()
		server.sendmail(from_address, to_address, text)
		server.sendmail(from_address, bcc_address, text)
		server.quit()

		print "REPORT EXCHNAGE", exchange, "READY AND SENT to ", email
	except Exception as e:
         logger.error(e)
         pass
