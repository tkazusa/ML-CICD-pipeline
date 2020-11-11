# preprocess
DATA_PATH=$(python setenv.py preprocess data_path)
PREPROCESSED_OUTPUT_PATH=$(python setenv.py preprocess preprocessed_output_path)

# train
IMAGE_URL=$(python setenv.py train train_url)
BATCH_SIZE=$(python setenv.py train hyper_parameter batch_size)
EPOCH=$(python setenv.py train hyper_parameter epoch)