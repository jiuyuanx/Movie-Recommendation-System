from flaskr.offlineeval import offline_eval
import numpy as np

def test_load_df():
    df = offline_eval.load_df()
    assert df is not None
    assert len(df.shape) == 2

def test_process_ts():
    valid_ids = ['2024-03-04T05:30:38', '2024-12-12T05:30:38', '2014-10-12T05:30:38']
    invalid_ids = ['adam', -1, 'James']
    for valid_id in valid_ids:
            try:
                res = offline_eval.process_ts(valid_id)
                assert res and isinstance(res, str)
            except:
                print('Error in function -- converting actual timestamps')
    for invalid_id in invalid_ids:
            try:
                res = offline_eval.process_ts(invalid_id)
                if len(res) > 1:
                    print('valid', res)
                    raise('Error in function -- should not convert non-timestamps', res)
            except:
                continue
                
def test_preprocess_data():
    df = offline_eval.load_df()
    df = offline_eval.preprocess_data(df)
    assert df is not None
    assert 'timestamp' in df.columns and  'user' in df.columns and  'item' in df.columns and  'rating' in df.columns 
    assert df.timestamp.dtype == 'datetime64[ns]'
    assert df.user.dtype == 'object'
    assert df.item.dtype == 'object'
    assert df.rating.dtype == 'float64'
    assert min(df.rating) == 1.0 and max(df.rating) == 5.0

def test_fit_and_compute_predictions():
    try:
        predictions = offline_eval.fit_and_compute_predictions()
        for prediction in predictions:
            user, item, r_ui, est = prediction[0], prediction[1], prediction[2], prediction[3]   
            assert type(user) is  str
            assert type(item) is str
            assert type(r_ui) is float
            assert type(est) is np.float64
    except:
        raise("Error in giving test predictions, look at fit and train_test_split")

def test_obtain_model_statistics():
    try:
        predictions = offline_eval.fit_and_compute_predictions()
        rmse, mae = offline_eval.obtain_model_statistics(predictions)
        assert rmse is not None
        assert mae is not None
    except:
        raise("RMSE and MAE need to be numerical values")

    