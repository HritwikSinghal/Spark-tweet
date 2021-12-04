import ast
import re

from flask import Flask, jsonify, request
from flask import render_template

app = Flask(__name__)

dataValues = []
categoryValues = []


@app.route("/")
def home():
    return render_template('index.html', dataValues=dataValues, categoryValues=categoryValues)


@app.route('/refreshData')
def refresh_graph_data():
    global dataValues, categoryValues
    print("labels now: " + str(dataValues))
    print("data now: " + str(categoryValues))
    # dataValues[0]=dataValues[0]+1
    return jsonify(dataValues=dataValues, categoryValues=categoryValues)


@app.route('/updateData', methods=['POST'])
def update_data_post():
    global dataValues, categoryValues
    if not request.form or 'data' not in request.form:
        return "error", 400
    categoryValues = ast.literal_eval(request.form['label'])

    for i, ele in enumerate(categoryValues):
        try:
            new_ele = re.findall(r'bytearray\(b\'(#.*)\'\)', ele)[0]
            print(new_ele)
            categoryValues[i] = new_ele
        except:
            continue

    dataValues = ast.literal_eval(request.form['data'])
    print(f"labels received: {str(categoryValues)}")
    print(f"data received: {str(dataValues)}")
    return "success", 201


if __name__ == "__main__":
    app.run(host='localhost', port=5001, debug=True)
