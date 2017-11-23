class Commons:
    split_filename = 'file_piece_'
    ack_preprocess = 'Preprocess Done'
    request_compute = 'Please Compute'
    finish_compute = 'Compute Done'
    request_result = 'Result?'
    ack_result = 'Here are the results'
    end_now = 'End now before too late'

def dfsWrapper(dfs_opt, filename):
    try:
        dfs_opt(filename)
    except:
        sleep(1)
        dfsWrapper(dfs_opt, filename)