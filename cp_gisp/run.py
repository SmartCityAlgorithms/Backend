import config
from model import load_model1, load_dict1
from flask import Flask, request, jsonify
import os
import logging
from logging.handlers import RotatingFileHandler
from time import strftime
from scipy.sparse import csr_matrix
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


app = Flask(__name__)

handler = RotatingFileHandler(filename='app.log', maxBytes=10000000, backupCount=10)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


@app.route("/", methods=["GET"])
def general():
    return """Welcome to GISP Recommendation API""", 200


@app.route("/api/get_range_money_grants_for_inn/", methods=["POST"])
def get_range_money_grants_for_inn():
    # initialize the data dictionary that will be returned from the
    # view
    data = {"success": False}
    print(request.get_json())
    # ensure an image was properly uploaded to our endpoint
    if request.method == "POST" and request.get_json():
        # Load data
        request_json = request.get_json()
        # Check secret API key
        bad_check_result_true = config.check_api_key(request_json)
        if bad_check_result_true:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            logger.warning(f'{dt} Exception: {bad_check_result_true}')
            data['result'] = bad_check_result_true
            return jsonify(data)

        # Get input data
        inn = int(request_json.get('inn', 0))
        grant_lst = request_json.get('kbk_list', [])

        id_to_itemid, id_to_userid, itemid_to_id, userid_to_id = load_dict1()
        model1 = load_model1()
        user_feat_lightfm = pd.read_csv('model1/user_feat_lightfm.csv', index_col=0)

        grant_lst_id = []
        for item in grant_lst:
            grant_lst_id.append(itemid_to_id[item])

        user_id = int(userid_to_id[inn])

        predictions = model1.predict(user_ids=user_id, item_ids=grant_lst_id,
                                     user_features=csr_matrix(user_feat_lightfm.values).tocsr(),
                                     num_threads=4)
        predictions_lst = [str(x) for x in predictions]

        data["success"], data["result"] = True, predictions_lst

    # return the data dictionary as a JSON response
    return jsonify(data)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=os.environ.get('PORT', config.app_port), use_reloader=False)