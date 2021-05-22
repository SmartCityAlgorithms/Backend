import pickle
import numpy as np
from scipy.sparse import csr_matrix
import pandas as pd


def load_dict():
    with open('model1/id_to_itemid.p', 'rb') as fp:
        id_to_itemid = pickle.load(fp)

    with open('model1/id_to_userid.p', 'rb') as fp:
        id_to_userid = pickle.load(fp)

    with open('model1/itemid_to_id.p', 'rb') as fp:
        itemid_to_id = pickle.load(fp)

    with open('model1/userid_to_id.p', 'rb') as fp:
        userid_to_id = pickle.load(fp)

    return id_to_itemid, id_to_userid, itemid_to_id, userid_to_id


def load_model1():
    return pickle.load(open('model1/model1.pickle', 'rb'))


if __name__ == '__main__':
    id_to_itemid, id_to_userid, itemid_to_id, userid_to_id = load_dict()

    test_item_ids = np.array([
        itemid_to_id['00701100130290059612'],
        itemid_to_id['00707010220059611241'],
        itemid_to_id['020041235Д5019522251'],
        itemid_to_id['02204023026794451530'],
    ])

    model1 = load_model1()

    user_feat_lightfm = pd.read_csv('model1/user_feat_lightfm.csv', index_col=0)
    predictions = model1.predict(user_ids=0, item_ids=test_item_ids,
                                 user_features=csr_matrix(user_feat_lightfm.values).tocsr(),
                                 num_threads=4)
    print(predictions)

    print(1)