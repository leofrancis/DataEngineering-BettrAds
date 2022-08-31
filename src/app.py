# from flask import Flask, render_template, request, jsonify, url_for
from flask import Flask, render_template
from manipulate_data import populate, report

app = Flask(__name__, template_folder='templates')

@app.route("/")
def index():
    return render_template('index.html')

@app.get("/populate")
def get_data():
    populate() 
    return render_template('populate.html')

@app.get("/report/")
@app.get("/report/<sql>")
def get_report(sql=None):
    if report(sql):
        return render_template('report.html') 
    else:
        return "Problema na geração do relatório"