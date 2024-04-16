from flaskr.trainmodel import train
import numpy as np

accepted_columns = ['user_id', 'movie_id', 'rate']

def test_pure_dataloader():
    try:
        df = train.read_dataloader()
        assert df is not None
        assert len(df.shape) == 2
        assert set(df.columns) == set(accepted_columns)
        assert df.user_id.dtype == 'int64'
        assert df.movie_id.dtype == 'object'
        assert df.rate.dtype == 'int64'
    except:
        raise Exception('Error in initilaizing dataloader')

def test_casted_dataloader():
    try:
        df = train.read_dataloader()
        casted_df = train.cast_dataloader(df)
        assert casted_df is not None
        assert len(df.shape) == 2
        assert set(df.columns) == set(accepted_columns)
        assert df.user_id.dtype == 'int64'
        assert df.movie_id.dtype == 'object'
        assert df.rate.dtype == 'int64'
    except:
        raise Exception('Error in casting dataloader')

def test_model_performance():
    try:
        df = train.read_dataloader()
        casted_df = train.cast_dataloader(df)
        measures, cv, verbose = ['RMSE', 'MAE'], 5, True
        threshold_tr_acc, threshold_test_acc, threshold_time = .7, .5, 5
        model, scores = train.cross_validate_model(casted_df, measures, cv, verbose)
        assert model is not None and scores is not None
        assert 'test_rmse' in scores and isinstance(scores['test_rmse'], np.ndarray)
        assert 'test_mae' in scores and isinstance(scores['test_mae'], np.ndarray)
        assert 'fit_time' in scores and isinstance(scores['fit_time'], tuple)
        assert 'test_time' in scores and isinstance(scores['test_time'], tuple)
        assert len(scores['test_rmse']) == cv and len(scores['test_mae']) == cv 
        assert any(map(lambda score: score >= threshold_tr_acc, scores['test_rmse']) )
        assert any(map(lambda score: score >= threshold_test_acc, scores['test_mae']) )
        assert any(map(lambda time: time <= threshold_time, scores['fit_time']) )
        assert any(map(lambda time: time <= threshold_time, scores['test_time']) )
    except:
        raise Exception('Error in model loading/performance')

def test_fit_model():
    try:
        df = train.read_dataloader()
        data = train.cast_dataloader(df)
        model, _ = train.cross_validate_model(data)
        train.fit_model(data, model)
    except:
        raise Exception('Error in fitting model')

def test_dump_model():
    try:
        df = train.read_dataloader()
        data = train.cast_dataloader(df)
        model, _ = train.cross_validate_model(data)
        train.fit_model(data, model)
        train.dump_model(model)
    except:
        raise Exception('Error in dumping model to path')

def test_model_statistics():
    try:
        df = train.read_dataloader()
        data = train.cast_dataloader(df)
        model, _ = train.cross_validate_model(data)
        train.fit_model(data, model)
        memory_info = train.generate_model_and_info()
        model_disk_size, approx_model_size_in_memory, accurate_model_size_in_memory = memory_info
        assert model_disk_size is not None or model_disk_size != 0
        assert approx_model_size_in_memory is not None or approx_model_size_in_memory != 0
        assert accurate_model_size_in_memory is not None or accurate_model_size_in_memory != 0
    except:
        raise Exception('Error in computing model statistics')
