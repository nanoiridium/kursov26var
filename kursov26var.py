from flask import Flask, render_template, request, url_for, redirect
import mysql.connector
app = Flask(__name__, template_folder="kursov26var_templates")

def DB_connect(us:str, passw:str):
    try:
        conn = mysql.connector.connect(
            user = us, 
            password = passw, 
            host = "127.0.0.1",
            database = "kursov26var"
            )
    except:
        conn = None
    return conn

conn = DB_connect("root", "root")
cursor = conn.cursor()

def checkReport(r_year):
    _SQL = """
        SELECT COUNT(*)
	        FROM kursov26var.report
		        WHERE report.year = %(reportYear)s;
    """
    cursor.execute(_SQL, {"reportYear": r_year})
    result = cursor.fetchall()
    return result[0][0]

@app.route('/', methods=['post', 'get'])
def menu():
    try:
        ref = request.args['ref']
    except:
        ref = None

    if (ref == '1'):
        return redirect(url_for('request1'))
    if (ref == '2'):
        return redirect(url_for('request2'))
    if (ref == '3'):
        return redirect(url_for('request3'))
    if (ref == '4'):
        return redirect(url_for('request4'))
    if (ref == '5'):
        return redirect(url_for('request5'))
    if (ref == '6'):
        return redirect(url_for('request6'))
    if (ref == '7'):
        return redirect(url_for('request7'))
    if (ref == '7_show'):
        return redirect(url_for('request7_show'))

    if(ref == 'exit'):
        return 0
    return render_template('menu.html')

@app.route('/request1', methods=['post', 'get'])
def request1():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT kursov26var.drugs.iddrug, kursov26var.drugs.drugName, SUM(kursov26var.requestslist.drugAmount) AS sumAmount
	        FROM kursov26var.requestslist 
		        JOIN kursov26var.request ON kursov26var.requestslist.idrequest = kursov26var.request.idrequest
                JOIN kursov26var.drugs ON kursov26var.requestslist.iddrug = kursov26var.drugs.iddrug
			        WHERE MONTH(kursov26var.request.requestDate) = 3 AND YEAR(kursov26var.request.requestDate) = 2017
				        GROUP BY kursov26var.drugs.iddrug;
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["iddrug", "drugName", "sumAmount"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request1.html", Data = res)

@app.route('/request2', methods=['post', 'get'])
def request2():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT kursov26var.provider.idprovider, kursov26var.provider.providerName, SUM(kursov26var.request.requestCost) AS totalCost
	        FROM kursov26var.requestslist 
		        JOIN kursov26var.request ON kursov26var.requestslist.idrequest = kursov26var.request.idrequest
                JOIN kursov26var.provider ON kursov26var.request.idprovider = kursov26var.provider.idprovider
			        WHERE MONTH(kursov26var.request.requestDate) = 3 AND YEAR(kursov26var.request.requestDate) = 2017
				        GROUP BY kursov26var.requestslist.idrequest;
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["idprovider", "providerName", "totalCost"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request2.html", Data = res)

@app.route('/request3', methods=['post', 'get'])
def request3():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    if (request.method == 'POST'):
        ID = request.form.get('ID')  

        data = (ID, ID)
        _SQL = """
            SELECT kursov26var.provider.idprovider, kursov26var.provider.providerName, kursov26var.provider.providerAddress, kursov26var.provider.contractDate
	            FROM kursov26var.requestslist 
		            JOIN kursov26var.request ON kursov26var.requestslist.idrequest = kursov26var.request.idrequest
                    JOIN kursov26var.provider ON kursov26var.request.idprovider = kursov26var.provider.idprovider
			            WHERE MONTH(kursov26var.request.requestDate) = 3 AND YEAR(kursov26var.request.requestDate) = 2017
                        AND kursov26var.requestslist.iddrug = %s
                        AND kursov26var.requestslist.drugPrice = (SELECT MAX(kursov26var.requestslist.drugPrice) 
														            FROM kursov26var.requestslist 
															            WHERE kursov26var.requestslist.iddrug = %s
																            GROUP BY kursov26var.requestslist.iddrug);
        """
        cursor.execute(_SQL, data)
        result = cursor.fetchall()
        scheme = ["idprovider", "providerName", "providerAddress", "contractDate"]
        for client in result:
            res.append(dict(zip(scheme, client)))
    return render_template("request3.html", Data = res)

@app.route('/request4', methods=['post', 'get'])
def request4():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT kursov26var.provider.idprovider, kursov26var.provider.providerName, kursov26var.provider.providerAddress, kursov26var.provider.contractDate
	        FROM kursov26var.requestslist 
		        JOIN kursov26var.request ON kursov26var.requestslist.idrequest = kursov26var.request.idrequest
                JOIN kursov26var.provider ON kursov26var.request.idprovider = kursov26var.provider.idprovider
			        WHERE MONTH(kursov26var.request.requestDate) BETWEEN 1 AND 6 AND YEAR(kursov26var.request.requestDate) = 2017
                    AND kursov26var.request.requestCost = (SELECT MAX(kursov26var.request.requestCost)
														        FROM kursov26var.requestslist
															        JOIN kursov26var.request ON kursov26var.requestslist.idrequest = kursov26var.request.idrequest
																        WHERE MONTH(kursov26var.request.requestDate) BETWEEN 1 AND 6 AND YEAR(kursov26var.request.requestDate) = 2017)
				        GROUP BY kursov26var.provider.idprovider;
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["idprovider", "providerName", "providerAddress", "contractDate"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request4.html", Data = res)

@app.route('/request5', methods=['post', 'get'])
def request5():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT kursov26var.provider.idprovider, kursov26var.provider.providerName, kursov26var.provider.providerAddress, kursov26var.provider.contractDate
	        FROM kursov26var.provider
		        LEFT JOIN kursov26var.request ON kursov26var.provider.idprovider = kursov26var.request.idprovider
			        WHERE kursov26var.request.idrequest IS NULL;
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["idprovider", "providerName", "providerAddress", "contractDate"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request5.html", Data = res)

@app.route('/request6', methods=['post', 'get'])
def request6():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT kursov26var.provider.idprovider, kursov26var.provider.providerName, kursov26var.provider.providerAddress, kursov26var.provider.contractDate
	        FROM kursov26var.provider
		        LEFT JOIN (SELECT * FROM kursov26var.requestslist
						        JOIN kursov26var.request USING(idrequest)
							        WHERE MONTH(kursov26var.request.requestDate) = 3 AND YEAR(kursov26var.request.requestDate) = 2017) AS tmp
		        ON kursov26var.provider.idprovider = tmp.idprovider
			        WHERE tmp.idrequest IS NULL;
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["idprovider", "providerName", "providerAddress", "contractDate"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request6.html", Data = res)

@app.route('/request7', methods=['post', 'get'])
def request7():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    isExist = -1
    if (request.method == 'POST'):
        r_year = request.form.get('r_year')
        if(checkReport(r_year) > 0):
            isExist = 1
        else:
            isExist = 0
        
        data = (r_year, 0)
        if(isExist == 0):
            result = cursor.callproc('report', data)
            conn.commit()

    return render_template("request7.html", state = isExist)

@app.route('/request7_show', methods=['post', 'get'])
def request7_show():
    try:
        ref = request.args['ref']
    except:
        ref = None
    if (ref == 'back'):
        return redirect(url_for('/'))

    res = []
    _SQL = """
        SELECT *
	        FROM kursov26var.report
    """
    cursor.execute(_SQL)
    result = cursor.fetchall()
    scheme = ["providerName", "avgIncome", "year"]
    for client in result:
        res.append(dict(zip(scheme, client)))
    return render_template("request7_show.html", Data = res)

app.run()