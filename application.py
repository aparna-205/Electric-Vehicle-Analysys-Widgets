import json
from flask import Flask, request, render_template
from flask import Flask, render_template, request, jsonify, flash, redirect,session
from flask_mysqldb  import MySQL, MySQLdb
import pandas as pd
# initialize flask function
import mysql.connector as sql
import numpy as np
import warnings
warnings.filterwarnings("ignore")
application = app = Flask(__name__)
db_connection = sql.connect(host='localhost', database='vehicledata1', user="****", password='****',auth_plugin='mysql_native_password')
app.secret_key = "caircocoders-ednalan"
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = '****'
app.config['MYSQL_PASSWORD'] = '****'
app.config['MYSQL_DB'] = "vehicledata1"
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
mysql = MySQL(app)
@app.route('/')
def hello_world():
    if 'username' in request.cookies and 'password' in request.cookies:
        username = request.cookies['username']
        password = request.cookies['password']
        return render_template("login.html", username=username, password=password)
    else:
        return render_template("login.html")
database = {'user': '4321'}
@app.route('/form_login', methods=['POST', 'GET'])
def login():
	name1 = request.form['username']
	pwd = request.form['password']
	if name1 not in database:
		return render_template('login.html',
							info='Invalid User ????!')
	else:
		if database[name1] != pwd:
			return render_template('login.html',
								info='Invalid Password ????!')
		else:
			return render_template('dashboard.html',
								name=name1)
@app.route('/login', methods=['POST', 'GET'])
def login1():
    return render_template('dashboard.html',
                           )
@app.route("/dashboard", methods=["GET", "POST"])
def get_odo():
    return render_template("index.html", c1result=None)
@app.route("/dashboard1", methods=["GET", "POST"])
def get_odo1():
    return render_template("index1.html", c1result=None)
@app.route("/range", methods=["POST", "GET"])
def range():
    formatted_num=None
    if request.method == "POST":
        start_date = request.form['start']
        end_date = request.form['end']
        selected_table = request.form.get('table_name')
        cur = mysql.connection.cursor()
        alltables = ['b1allsaints',"b1rajkamalscreations003"]
        results = []
        for total in alltables:
            query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(total)
            cur.execute(query, (start_date, end_date))
            table_data = cur.fetchall()
            df = pd.DataFrame(table_data)
            df = df[(df['IGN'] == 'On') & (df['GPS'] == 'On')]
            df2 = df.drop_duplicates(subset=['Odometer'])
            df1 = df2[df2['Odometer'] != 0]
            df1["Odometer"] = df1["Odometer"].diff()
            df3 = df1.groupby(df1.date_time.dt.date)['Odometer'].sum()
            result = df3.sum() / len(df3) * 0.001
            formatted_num = "{:.2f}".format(result)
            results.append((result, total))
            results.sort(reverse=True)
            top_3_results = results[:3]
            data = [
                {"data": item[0], "label": item[1]} for item in top_3_results
            ]
            json_data = json.dumps(data)
        if selected_table == 'all':
            selected_tables = ['b1allsaints', "b1rajkamalscreations003"]
        else:
            selected_tables = [selected_table]
        total_sum = 0
        id_count = 0
        counts = []
        individual_results = []  # Store individual results for each selected table
        for table in selected_tables:
              query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(table)
              cur.execute(query, (start_date, end_date))
              table_data = cur.fetchall()
              df = pd.DataFrame(table_data)
              print(df)
              dfk= pd.DataFrame(table_data)
              dfo= pd.DataFrame(table_data)
              dfh= pd.DataFrame(table_data)
              df.rename(columns={'Data Actual Time': 'date_time'}, inplace=True)
              df = df[(df['IGN'] == 'On') & (df['GPS'] == 'On')]
              df2 = df.drop_duplicates(subset=['Odometer'])
              df1 = df2[df2['Odometer'] != 0]
              df1["Odometer"] = df1["Odometer"].diff()
              df3 = df1.groupby(df1.date_time.dt.date)['Odometer'].sum()
              result = df3.sum() / len(df3)* 0.001
              formatted_num = "{:.2f}".format(result)
              total_sum += result
              individual_results.append(result)
              counts.append((id_count, table))
              cycle_count = 0
              previous_state = None
              for i, row in dfk.iterrows():
                current_state = row["IGN"]
                if previous_state == "On" and current_state == "Off":
                    cycle_count += 1
                previous_state = current_state
              cycle_count1 = 0
              previous_state1 = None
              for i, row in dfk.iterrows():
                current_state1 = row["IGN"]
                if previous_state1 == "Off" and current_state1 == "On":
                    cycle_count1 += 1
                previous_state1 = current_state1
              dfm= df[df['Speed'] != 0]
              grouped = dfm.groupby(dfm.date_time.dt.date)
              dfn = df[df['Speed'] <= 35]
              grouped1 = dfn.groupby(dfn.date_time.dt.date)
              top_speed = grouped1["Speed"].max()
              avg_speed = grouped["Speed"].mean()
              df_json = avg_speed.to_json(date_format='iso')
              df1_json = top_speed.to_json(date_format="iso")
              df22 = dfo.drop_duplicates(subset=['Odometer'])
              df11= df22[df22['Odometer'] != 0]
              df11["Odometer"] = df11["Odometer"].diff()
              df11['Time'] = pd.to_datetime(df11['date_time'], format='%d-%m-%Y %H:%M:%S')
              df11['Date'] = df11['Time'].dt.date
              df11['Time'] = df11['Time'].dt.time
              df11['Time'] = pd.to_datetime(df11['date_time'], format='%d-%m-%Y %H:%M:%S')
              df11['Weekday'] = df11['Time'].dt.strftime('%A')  # Convert date to weekday
              df11['Date'] = df11['Time'].dt.strftime('%Y-%m-%d')  # Convert date to YYYY-MM-DD format
              df11['Time'] = pd.to_datetime(df11['Time'], format='%d-%m-%Y %H:%M:%S')
              df11['Time'] = df11['Time'].dt.strftime('%H:00')
              df11["Odometer"] = df11["Odometer"] * 0.001
              df11['Date_Weekday'] = df11['Date'].astype(str) + ' (' + df11['Weekday'] + ')'
              pivot_table = df11.pivot_table(values='Odometer', index='Date_Weekday', columns='Time', aggfunc='sum')
              pivot_table_json = pivot_table.to_json(orient='columns')
              op_col = []
              for i in dfh['Speed']:
                  op_col.append(i)
              np.set_printoptions(threshold=np.inf)
              lower_limit = int(request.form.get('lower_limit', 0))
              upper_limit1 = int(request.form.get('upper_limit1', 0))
              upper_limit2 = int(request.form.get('upper_limit2', 0))
              x = np.array(op_col)
              x1 = x.astype('int32')
              sub_lists = np.split(x1, np.where(np.diff(x1) < 0)[0] + 1)
              id_count = 0
              for unit in sub_lists:
                  if min(unit) <= lower_limit and max(unit) > upper_limit1 and max(unit) < upper_limit2 and len(
                          set(unit)) > 1:
                      id_count += 1
        average_result = sum(individual_results) / len(individual_results)
        formatted_average_result = "{:.2f}".format(average_result)
        return jsonify({'htmlresponse': render_template('odo.html',average_result=average_result, c1result=formatted_num,data=df_json,data1=df1_json,count_result=id_count,cycle_count=cycle_count,cycle_count1=cycle_count1,json_data=json_data,pivot_table_json=pivot_table_json,id_count=id_count,results=results,)})
@app.route("/count", methods=["POST"])
def count():
        if request.method == "POST":
            start_date = request.form['start']
            end_date = request.form['end']
            selected_table = request.form.get('table_name')
            cur = mysql.connection.cursor()
            alltables = ['b1allsaints', "b1rajkamalscreations003"]

            results = []
            for total in alltables:
                query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(total)
                cur.execute(query, (start_date, end_date))
                table_data = cur.fetchall()
                df = pd.DataFrame(table_data)
                op_col = []
                for i in df['Speed']:
                    op_col.append(i)
                np.set_printoptions(threshold=np.inf)
                lower_limit = int(request.form.get('lower_limit', 0))
                upper_limit1 = int(request.form.get('upper_limit1', 0))
                upper_limit2 = int(request.form.get('upper_limit2', 0))
                print(lower_limit)
                print(upper_limit1)
                print(upper_limit2)
                x = np.array(op_col)
                x1 = x.astype('int32')
                sub_lists = np.split(x1, np.where(np.diff(x1) < 0)[0] + 1)
                id_count = 0
                for unit in sub_lists:
                    if min(unit) <= lower_limit and max(unit) > upper_limit1 and max(unit) < upper_limit2 and len(
                            set(unit)) > 1:
                        id_count += 1
                results.append((id_count, total))
                results.sort(reverse=True)
                top_3_results = results[:3]
                data = [
                    {"data": item[0], "label": item[1]} for item in top_3_results
                ]
                json_data1 = json.dumps(data)

            if selected_table == 'all':
                selected_tables = ['b1allsaints', "b1rajkamalscreations003"]

            else:
                selected_tables = [selected_table]
            id_count = 0
            for table in selected_tables:
                query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(table)
                cur.execute(query, (start_date, end_date))
                table_data = cur.fetchall()
                df = pd.DataFrame(table_data)
                op_col = []
                for i in df['Speed']:
                    op_col.append(i)
                np.set_printoptions(threshold=np.inf)
                lower_limit = int(request.form.get('lower_limit', 0))
                upper_limit1 = int(request.form.get('upper_limit1', 0))
                upper_limit2 = int(request.form.get('upper_limit2', 0))
                print(lower_limit)
                print(upper_limit1)
                print(upper_limit2)
                x = np.array(op_col)
                x1 = x.astype('int32')
                sub_lists = np.split(x1, np.where(np.diff(x1) < 0)[0] + 1)
                id_count = 0
                for unit in sub_lists:
                    if min(unit) <= lower_limit and max(unit) > upper_limit1 and max(unit) < upper_limit2 and len(
                            set(unit)) > 1:
                        id_count += 1
            return jsonify({'htmlresponse': render_template('acceleration.html', id_count=id_count, results=results,json_data1=json_data1)})
if __name__ == '__main__':
 app.run(debug=True, port="3116")
