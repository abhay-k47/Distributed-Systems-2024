from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200
    
@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"message":"Internal server Error: check params"}),500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
