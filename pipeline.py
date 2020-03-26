import uuid
import logging
import stepfunctions
import boto3
import sagemaker

from sagemaker.amazon.amazon_estimator import get_image_uri
from stepfunctions import steps
from stepfunctions.inputs import ExecutionInput
from stepfunctions.workflow import Workflow

stepfunctions.set_stream_logger(level=logging.INFO)

BATCH_ROLE='arn:aws:iam::815969174475:role/service-role/AWSBatchServiceRole'
WORKFLOW_ROLE='arn:aws:iam::815969174475:role/StepFunctionsWorkflowExecutionRole'
REGION='us-east-1'
BUCKET='SFn-flow'

id = uuid.uuid4().hex

# SFn の実行に必要な情報を渡す際のスキーマを定義します
execution_input = ExecutionInput(schema={
    'BatchJobDefinition': str,
    'BatchJobName': str,
    'BatchJobQueue': str
    }
)

# SFn のワークフローの定義を記載します
inputs={
    # AWS Batch
    'BatchJobDefinition': 'job_run:1',
    'BatchJobName': 'test',
    'BatchJobQueue': 'test'
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

workflow_definition = steps.Chain([etl_step])
workflow = Workflow(
    name='flow_{}'.format(id),
    definition=workflow_definition,
    role=WORKFLOW_ROLE,
    execution_input=execution_input
)


workflow.create()
execution = workflow.execute(inputs=inputs)
