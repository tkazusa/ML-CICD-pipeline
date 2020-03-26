import uuid
import logging
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
TRAINING_JOB_NAME='sf-train-{}'.format(id) # JobNameの重複NG
BATCH_ROLE='arn:aws:iam::815969174475:role/service-role/AWSBatchServiceRole'
SAGEMAKER_ROLE = 'arn:aws:iam::815969174475:role/service-role/AmazonSageMaker-ExecutionRole-20190909T195854'
WORKFLOW_ROLE='arn:aws:iam::815969174475:role/StepFunctionsWorkflowExecutionRole'
TRAIN_URI='815969174475.dkr.ecr.us-east-1.amazonaws.com/sm-tf-nightly-gpu:latest'


# SFn の実行に必要な情報を渡す際のスキーマを定義します
execution_input = ExecutionInput(schema={
    # AWS Batch
    'BatchJobDefinition': str,
    'BatchJobName': str,
    'BatchJobQueue': str,

    # SageMaker
    'TrainJobName': str,
    }
)

# SFn のワークフローの定義を記載します
inputs={
    # AWS Batch
    'BatchJobDefinition': 'job_run:1',
    'BatchJobName': 'test',
    'BatchJobQueue': 'test',

    'TrainJobName': TRAINING_JOB_NAME
    }


# それぞれのステップを定義していきます
## AWS Batch のジョブを Subtit するステップ
etl_step = steps.BatchSubmitJobStep(
    'Execute AWS Batch job',
    parameters={
        "JobDefinition":execution_input['BatchJobDefinition'],
        "JobName": execution_input['BatchJobName'],
        "JobQueue": execution_input['BatchJobQueue'] 
        }
)

## SageMaker の学習ジョブを実行するステップ
hyperparameters = {'batch_size': 64,'epochs': 1}
output_path = 's3://{}/output'.format(BUCKET)
data_path = {'train': 's3://{}/data'.format(BUCKET)}

estimator = Estimator(image_name=TRAIN_URI,
                      role=SAGEMAKER_ROLE,
                      hyperparameters=hyperparameters,
                      train_instance_count=1,
                      train_instance_type='ml.p2.xlarge',
                      output_path=output_path)


training_step = steps.TrainingStep(
    'Train Step', 
    estimator=estimator,
    data=data_path,
    job_name=execution_input['TrainJobName'],  
    wait_for_completion=False
)

# 各 Step を連結
workflow_definition = steps.Chain(
    [
        etl_step,
        training_step
    ]
)

# Workflow の作成
workflow = Workflow(
    name=FLOW_NAME,
    definition=workflow_definition,
    role=WORKFLOW_ROLE,
    execution_input=execution_input
)
workflow.create()

# Workflow の実行
execution = workflow.execute(inputs=inputs)
