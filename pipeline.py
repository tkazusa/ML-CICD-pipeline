import os
import uuid
import logging
import argparse

import stepfunctions
import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.estimator import Estimator

from stepfunctions import steps
from stepfunctions.inputs import ExecutionInput
from stepfunctions.workflow import Workflow

stepfunctions.set_stream_logger(level=logging.INFO)
id = uuid.uuid4().hex

REGION='us-east-1'
BUCKET='sfn-sagemaker-workflow'
FLOW_NAME='flow_{}'.format(id) 
id = uuid.uuid4().hex

# JobNameの重複NGなのでidを追加している
PREPROCESSING_JOB_NAME='sm-processing-{}'.format(id)
TRAINING_JOB_NAME='sm-train-{}'.format(id)

SAGEMAKER_ROLE = 'arn:aws:iam::815969174475:role/service-role/AmazonSageMaker-ExecutionRole-20190909T195854'
WORKFLOW_ROLE='arn:aws:iam::815969174475:role/StepFunctionsWorkflowExecutionRole'

def create_estimator():
    hyperparameters = {'batch_size': args.batch_size,'epochs': args.epoch}
    output_path = 's3://{}/output'.format(BUCKET)
    estimator = Estimator(image_name=args.image_url,
                        role=SAGEMAKER_ROLE,
                        hyperparameters=hyperparameters,
                        train_instance_count=1,
                        train_instance_type='ml.p2.xlarge',
                        output_path=output_path)
    return estimator


if __name__ == '__main__':
    # flow.yaml の定義を環境変数経由で受け取る
    # buildspec.yaml の ENV へ直接書き込んでも良いかも
    parser.add_argument('--image_url', type=str, default=os.environ['IMAGE_URL'])
    parser.add_argument('--data_path', type=str, default=os.environ['DATA_PATH'])
    parser.add_argument('--preprocessed_output_path', type=str,
default=os.environ['PREPROCESSED_OUTPUT_PATH'])
    parser.add_argument('--batch_size', type=str, default=os.environ['BATCH_SIZE'])
    parser.add_argument('--epoch', type=str, default=os.environ['EPOCH'])
    args = parser.parse_args()


    execution_input = ExecutionInput(schema={
        # SageMaker processing*
        'ProcessingJobName': str,
        # SageMaker Training
        'TrainJobName': str}
    )

    # それぞれのステップを定義していきます
    ## SageMaker の前処理を実行するステップ
    
    processing_inputs = [
        ProcessingInput(
            source=args.data_path,
            destination="/opt/ml/processing/input",
            input_name="input-data"
        ),
    ]

    processing_outputs = [
        # 処理後のデータをdestinationで指定した S3 パスへ退避、複数指定可能。
        ProcessingOutput(
            source="/opt/ml/processing/output/",
            destination=args.preprocessed_output_path,
            output_name="train_data",
        )
    ]
    

    pre_processor = Processor(
        role=SAGEMAKER_ROLE, 
        image_uri=processing_image_uri,
        entrypoint=["python3", "/opt/ml/preprocess/preprocess.py",],
        instance_count=1, 
        instance_type="ml.p3.2xlarge")    
    

    processing_step = ProcessingStep(
        "SageMaker pre-processing step",
        processor=pre_processor,
        job_name=execution_input["ProcessingJobName"],
        inputs=processing_inputs,
        outputs=processing_outputs,
        container_entrypoint=["python3", "/opt/ml/preprocess/preprocess.py"], # Dockerfileでスクリプトをイメージ内へコピーしたパス
    )    
    
    
    ## SageMaker の学習ジョブを実行するステップ
    estimator = create_estimator()
    data_path = {'train':
                 "{}/{}".format(args.preprocessed_output_path,"train")} # Preprocess が終わった後に S3 に吐き出したパス (今回は便宜上、前処理と学習と同じものを使用、詳細は flow.yaml を参照下さい。)

    training_step = steps.TrainingStep(
        'Train Step', 
        estimator=estimator,
        data=data_path,
        job_name=execution_input['TrainJobName'],  
        wait_for_completion=False  # SFnを実行した後に Bitbucket へプルリクを上げるように変更したため、ここは True で良いかも。
    )

    # 各 Step を連結
    chain_list = [processing_step, training_step]
    workflow_definition = steps.Chain(chain_list)

    # Workflow の作成
    workflow = Workflow(
        name=FLOW_NAME,
        definition=workflow_definition,
        role=WORKFLOW_ROLE,
        execution_input=execution_input
    )
    workflow.create()

    # Workflow の実行
    # SFn のワークフローの定義を記載します
    inputs={
        # SageMaker Processing
        'ProcessingJobName': PREPROCESSING_JOB_NAME,
        # SageMaker Training
        'TrainJobName': TRAINING_JOB_NAME
    }
    
    execution = workflow.execute(inputs=inputs)
